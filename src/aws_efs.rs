use std::fmt;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::sign::v4;
use rcgen::{
    CertificateParams, CustomExtension, DistinguishedName, DnType, KeyPair, PKCS_RSA_SHA256,
    PublicKeyData, RsaKeySize,
};
use rustls::sign::CertifiedKey;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use time::{Duration, OffsetDateTime, UtcOffset};
use yasna::Tag;
use yasna::models::UTCTime;
use zeroize::Zeroizing;

use crate::error::Error;

const EFS_SERVICE_NAME: &str = "elasticfilesystem";
const EFS_CERT_NOT_BEFORE_SKEW: Duration = Duration::minutes(15);
const EFS_CERT_LIFETIME: Duration = Duration::hours(3);
const EFS_SIGV4_EXPIRES_SECONDS: u64 = 24 * 60 * 60;

const EFS_ACCESS_POINT_ID_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 4843, 7, 1];
const EFS_CLIENT_AUTH_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 4843, 7, 2];
const EFS_FILE_SYSTEM_ID_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 4843, 7, 3];
const SUBJECT_KEY_IDENTIFIER_OID: &[u64] = &[2, 5, 29, 14];

/// AWS EFS IAM authorization settings.
///
/// This is only needed for EFS file systems whose resource policy requires
/// IAM authorization for NFS clients. It generates the EFS-specific client
/// certificate used by the TLS handshake; NFS RPCs still use AUTH_SYS.
#[derive(Clone)]
pub struct EfsIamConfig {
    file_system_id: String,
    region: String,
    credentials_provider: SharedCredentialsProvider,
    access_point_id: Option<String>,
}

impl fmt::Debug for EfsIamConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EfsIamConfig")
            .field("file_system_id", &self.file_system_id)
            .field("region", &self.region)
            .field("credentials_provider", &"<redacted>")
            .field("access_point_id", &self.access_point_id)
            .finish()
    }
}

impl EfsIamConfig {
    /// Creates an EFS IAM configuration from an AWS credential provider.
    ///
    /// `file_system_id` is the `fs-...` id, not the DNS endpoint. `region` is
    /// the AWS region of the file system.
    pub fn new(
        file_system_id: impl Into<String>,
        region: impl Into<String>,
        credentials_provider: impl ProvideCredentials + 'static,
    ) -> Self {
        Self {
            file_system_id: file_system_id.into(),
            region: region.into(),
            credentials_provider: SharedCredentialsProvider::new(credentials_provider),
            access_point_id: None,
        }
    }

    /// Returns the configured EFS file system id.
    pub fn file_system_id(&self) -> &str {
        &self.file_system_id
    }

    /// Returns the configured AWS region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns the configured EFS access point id, if any.
    pub fn access_point_id(&self) -> Option<&str> {
        self.access_point_id.as_deref()
    }

    /// Uses an EFS access point for IAM authorization.
    pub fn with_access_point_id(mut self, access_point_id: impl Into<String>) -> Self {
        self.access_point_id = Some(access_point_id.into());
        self
    }

    pub(crate) async fn tls_client_auth_material(
        &self,
        common_name: &str,
    ) -> Result<EfsIamTlsMaterial, Error> {
        self.validate()?;
        if common_name.trim().is_empty() {
            return Err(Error::invalid_config(
                "EFS IAM certificate common name must not be empty",
            ));
        }
        let now = truncate_system_time(SystemTime::now())?;
        self.tls_client_auth_material_at(common_name, now).await
    }

    async fn tls_client_auth_material_at(
        &self,
        common_name: &str,
        now: OffsetDateTime,
    ) -> Result<EfsIamTlsMaterial, Error> {
        let credentials = self
            .credentials_provider
            .provide_credentials()
            .await
            .map_err(|err| {
                Error::invalid_config(format!("failed to load AWS credentials: {err}"))
            })?;
        validate_credentials(&credentials, now)?;

        // RSA-3072 keygen and self-signing are heavy CPU work that would
        // stall the runtime worker for the whole connect.
        let config = self.clone();
        let common_name = common_name.to_owned();
        tokio::task::spawn_blocking(move || {
            let key_pair = Zeroizing::new(generate_key_pair().map_err(|err| {
                Error::invalid_config(format!("failed to generate EFS IAM key: {err}"))
            })?);
            let public_key_hash = public_key_sha1_hex(&key_pair);
            let signature = config.sign_connect(&credentials, &public_key_hash, now);
            let certificate_der =
                config.certificate_der(&key_pair, &common_name, &credentials, &signature, now)?;
            let private_key_der = key_pair.serialize_der();

            Ok(EfsIamTlsMaterial {
                certificate_der,
                private_key_der: Zeroizing::new(private_key_der),
            })
        })
        .await
        .map_err(|err| Error::protocol(format!("EFS IAM TLS material task failed: {err}")))?
    }

    fn validate(&self) -> Result<(), Error> {
        if !is_valid_file_system_id(&self.file_system_id) {
            return Err(Error::invalid_config(
                "EFS IAM file_system_id must be fs- followed by 8 or 17 lowercase hex digits",
            ));
        }
        if !is_valid_region(&self.region) {
            return Err(Error::invalid_config(
                "EFS IAM region must contain lowercase ASCII letters, digits, and interior hyphens",
            ));
        }
        if self
            .access_point_id
            .as_deref()
            .is_some_and(|access_point_id| !is_valid_access_point_id(access_point_id))
        {
            return Err(Error::invalid_config(
                "EFS IAM access_point_id must match fsap- followed by 17 lowercase hex digits",
            ));
        }
        Ok(())
    }

    fn sign_connect(
        &self,
        credentials: &Credentials,
        public_key_hash: &str,
        now: OffsetDateTime,
    ) -> String {
        sign_efs_connect(
            &self.file_system_id,
            &self.region,
            credentials,
            public_key_hash,
            now,
        )
    }

    fn certificate_der(
        &self,
        key_pair: &KeyPair,
        common_name: &str,
        credentials: &Credentials,
        signature: &str,
        now: OffsetDateTime,
    ) -> Result<Vec<u8>, Error> {
        let not_after = certificate_not_after(credentials, now)?;
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, common_name);

        let mut params = CertificateParams::default();
        params.not_before = now - EFS_CERT_NOT_BEFORE_SKEW;
        params.not_after = not_after;
        params.distinguished_name = distinguished_name;
        params.custom_extensions = self.custom_extensions(key_pair, credentials, signature, now);

        let certificate = params.self_signed(key_pair).map_err(|err| {
            Error::invalid_config(format!("failed to generate EFS IAM certificate: {err}"))
        })?;
        Ok(certificate.der().to_vec())
    }

    fn custom_extensions(
        &self,
        key_pair: &KeyPair,
        credentials: &Credentials,
        signature: &str,
        now: OffsetDateTime,
    ) -> Vec<CustomExtension> {
        let mut extensions = Vec::with_capacity(4);
        extensions.push(subject_key_identifier_extension(key_pair));
        if let Some(access_point_id) = &self.access_point_id {
            extensions.push(utf8_extension(EFS_ACCESS_POINT_ID_OID, access_point_id));
        }
        extensions.push(efs_client_auth_extension(credentials, signature, now));
        extensions.push(utf8_extension(EFS_FILE_SYSTEM_ID_OID, &self.file_system_id));
        extensions
    }
}

fn generate_key_pair() -> Result<KeyPair, rcgen::Error> {
    KeyPair::generate_rsa_for(&PKCS_RSA_SHA256, RsaKeySize::_3072)
}

pub(crate) struct EfsIamTlsMaterial {
    certificate_der: Vec<u8>,
    private_key_der: Zeroizing<Vec<u8>>,
}

impl EfsIamTlsMaterial {
    pub(crate) fn into_rustls(mut self) -> Result<CertifiedKey, Error> {
        let certificate_der = std::mem::take(&mut self.certificate_der);
        let private_key_der = std::mem::take(&mut *self.private_key_der);
        let private_key_der = Zeroizing::new(PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
            private_key_der,
        )));
        let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&private_key_der)
            .map_err(|err| {
                Error::invalid_config(format!("failed to parse generated EFS IAM key: {err}"))
            })?;
        Ok(CertifiedKey::new(
            vec![CertificateDer::from(certificate_der)],
            signing_key,
        ))
    }
}

fn validate_credentials(credentials: &Credentials, now: OffsetDateTime) -> Result<(), Error> {
    if credentials.access_key_id().is_empty() {
        return Err(Error::invalid_config(
            "EFS IAM AWS access key id must not be empty",
        ));
    }
    if credentials.secret_access_key().is_empty() {
        return Err(Error::invalid_config(
            "EFS IAM AWS secret access key must not be empty",
        ));
    }
    if let Some(expiry) = credentials.expiry() {
        let expiry = truncate_system_time(expiry)?;
        if expiry <= now {
            return Err(Error::invalid_config("EFS IAM AWS credentials are expired"));
        }
    }
    Ok(())
}

fn certificate_not_after(
    credentials: &Credentials,
    now: OffsetDateTime,
) -> Result<OffsetDateTime, Error> {
    let default_not_after = now + EFS_CERT_LIFETIME;
    let Some(expiry) = credentials.expiry() else {
        return Ok(default_not_after);
    };
    let expiry = truncate_system_time(expiry)?;
    if expiry <= now {
        return Err(Error::invalid_config("EFS IAM AWS credentials are expired"));
    }
    Ok(default_not_after.min(expiry))
}

/// `now` must be a whole-second UTC timestamp, as produced by
/// [`truncate_system_time`].
fn sign_efs_connect(
    file_system_id: &str,
    region: &str,
    credentials: &Credentials,
    public_key_hash: &str,
    now: OffsetDateTime,
) -> String {
    let credential_scope = credential_scope(now, region, EFS_SERVICE_NAME);
    let canonical_request = efs_canonical_request(
        file_system_id,
        credentials,
        public_key_hash,
        &credential_scope,
        now,
    );
    let string_to_sign = efs_string_to_sign(&canonical_request, &credential_scope, now);
    let signing_key = v4::generate_signing_key(
        credentials.secret_access_key(),
        SystemTime::from(now),
        region,
        EFS_SERVICE_NAME,
    );
    v4::calculate_signature(signing_key, string_to_sign.as_bytes())
}

fn efs_canonical_request(
    file_system_id: &str,
    credentials: &Credentials,
    public_key_hash: &str,
    credential_scope: &str,
    now: OffsetDateTime,
) -> String {
    let credential = efs_query_encode(&format!(
        "{}/{}",
        credentials.access_key_id(),
        credential_scope
    ));
    let mut query_params = vec![
        ("Action", "Connect".to_owned()),
        ("PublicKeyHash", efs_query_encode(public_key_hash)),
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
        ("X-Amz-Credential", credential),
        ("X-Amz-Date", efs_query_encode(&format_sigv4_datetime(now))),
        ("X-Amz-Expires", EFS_SIGV4_EXPIRES_SECONDS.to_string()),
        ("X-Amz-SignedHeaders", "host".to_owned()),
    ];
    if let Some(session_token) = credentials.session_token() {
        query_params.push(("X-Amz-Security-Token", efs_query_encode(session_token)));
    }
    query_params.sort_by(|left, right| left.0.cmp(right.0));
    let canonical_query = query_params
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let payload_hash = sha256_hex(b"");

    format!("GET\n/\n{canonical_query}\nhost:{file_system_id}\nhost\n{payload_hash}")
}

fn efs_string_to_sign(
    canonical_request: &str,
    credential_scope: &str,
    now: OffsetDateTime,
) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        format_sigv4_datetime(now),
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    )
}

fn credential_scope(now: OffsetDateTime, region: &str, service: &str) -> String {
    format!("{}/{region}/{service}/aws4_request", format_sigv4_date(now))
}

fn efs_client_auth_extension(
    credentials: &Credentials,
    signature: &str,
    now: OffsetDateTime,
) -> CustomExtension {
    CustomExtension::from_oid_content(
        EFS_CLIENT_AUTH_OID,
        yasna::construct_der(|writer| {
            writer.write_sequence(|writer| {
                writer.next().write_utf8_string(credentials.access_key_id());
                writer.next().write_bytes(signature.as_bytes());
                writer.next().write_utctime(&UTCTime::from_datetime(now));
                if let Some(session_token) = credentials.session_token() {
                    writer.next().write_tagged(Tag::context(0), |writer| {
                        writer.write_utf8_string(session_token);
                    });
                }
            });
        }),
    )
}

fn utf8_extension(oid: &[u64], value: &str) -> CustomExtension {
    CustomExtension::from_oid_content(
        oid,
        yasna::construct_der(|writer| writer.write_utf8_string(value)),
    )
}

fn subject_key_identifier_extension(key_pair: &KeyPair) -> CustomExtension {
    let public_key_hash = public_key_sha1(key_pair);
    CustomExtension::from_oid_content(
        SUBJECT_KEY_IDENTIFIER_OID,
        yasna::construct_der(|writer| writer.write_bytes(&public_key_hash)),
    )
}

fn public_key_sha1(key_pair: &KeyPair) -> [u8; 20] {
    Sha1::digest(key_pair.der_bytes()).into()
}

fn public_key_sha1_hex(key_pair: &KeyPair) -> String {
    hex_lower(&public_key_sha1(key_pair))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_lower(&digest)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn efs_query_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                output.push(byte as char)
            }
            _ => {
                output.push('%');
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    output
}

fn format_sigv4_date(time: OffsetDateTime) -> String {
    format!(
        "{:04}{:02}{:02}",
        time.year(),
        u8::from(time.month()),
        time.day()
    )
}

fn format_sigv4_datetime(time: OffsetDateTime) -> String {
    format!(
        "{}T{:02}{:02}{:02}Z",
        format_sigv4_date(time),
        time.hour(),
        time.minute(),
        time.second()
    )
}

fn truncate_system_time(time: SystemTime) -> Result<OffsetDateTime, Error> {
    OffsetDateTime::from(time)
        .to_offset(UtcOffset::UTC)
        .replace_nanosecond(0)
        .map_err(|err| Error::invalid_config(format!("invalid EFS IAM timestamp: {err}")))
}

fn is_valid_file_system_id(value: &str) -> bool {
    let Some(id) = value.strip_prefix("fs-") else {
        return false;
    };
    matches!(id.len(), 8 | 17) && id.bytes().all(is_lower_hex)
}

fn is_valid_access_point_id(value: &str) -> bool {
    let Some(id) = value.strip_prefix("fsap-") else {
        return false;
    };
    id.len() == 17 && id.bytes().all(is_lower_hex)
}

fn is_lower_hex(byte: u8) -> bool {
    matches!(byte, b'0'..=b'9' | b'a'..=b'f')
}

fn is_valid_region(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_time() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_704_164_645).unwrap()
    }

    #[test]
    fn efs_iam_signature_matches_efs_utils_algorithm() {
        let credentials = Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("session/token+example=".to_owned()),
            None,
            "test",
        );
        let signature = sign_efs_connect(
            "fs-1234567890abcdef0",
            "us-east-1",
            &credentials,
            "0123456789abcdef0123456789abcdef01234567",
            fixed_time(),
        );

        assert_eq!(
            signature,
            "69ba34b78fcea59cdbdb9e6d0324c41a5d1986e1950a4de0b86960201ac6bbdd"
        );
    }

    #[test]
    fn efs_client_auth_extension_matches_openssl_octetstring_encoding() {
        let credentials = Credentials::new("AKID", "SECRET", None, None, "test");
        let extension = efs_client_auth_extension(&credentials, "0123456789abcdef", fixed_time());

        assert_eq!(
            hex_lower(extension.content()),
            "30270c04414b4944041030313233343536373839616263646566170d3234303130323033303430355a"
        );
    }

    #[test]
    fn efs_client_auth_extension_encodes_session_token_as_explicit_zero() {
        let credentials = Credentials::new(
            "AKID",
            "SECRET",
            Some("session-token".to_owned()),
            None,
            "test",
        );
        let extension = efs_client_auth_extension(&credentials, "00", fixed_time());
        let encoded = hex_lower(extension.content());

        assert!(encoded.contains("a00f0c0d73657373696f6e2d746f6b656e"));
    }

    #[test]
    fn debug_output_redacts_the_credentials_provider() {
        let credentials = Credentials::new(
            "AKID_SHOULD_NOT_APPEAR",
            "SECRET_SHOULD_NOT_APPEAR",
            Some("TOKEN_SHOULD_NOT_APPEAR".to_owned()),
            None,
            "test",
        );
        let config = EfsIamConfig::new("fs-12345678", "us-east-1", credentials);

        let debug = format!("{config:?}");
        assert!(debug.contains("credentials_provider: \"<redacted>\""));
        assert!(!debug.contains("SHOULD_NOT_APPEAR"));
    }

    #[test]
    fn generated_tls_material_builds_a_matching_certified_key() {
        let key_pair = Zeroizing::new(generate_key_pair().unwrap());
        let certificate = CertificateParams::default()
            .self_signed(&*key_pair)
            .unwrap();
        let material = EfsIamTlsMaterial {
            certificate_der: certificate.der().to_vec(),
            private_key_der: Zeroizing::new(key_pair.serialize_der()),
        };

        material.into_rustls().unwrap().keys_match().unwrap();
    }

    #[test]
    fn validate_rejects_bad_ids() {
        let credentials = Credentials::new("AKID", "SECRET", None, None, "test");
        let err = EfsIamConfig::new("not-fs", "us-east-1", credentials.clone())
            .validate()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidConfig(_)));

        let err = EfsIamConfig::new("fs-1234abcd", "us-east-1", credentials)
            .with_access_point_id("fsap-notvalid")
            .validate()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidConfig(_)));

        let credentials = Credentials::new("AKID", "SECRET", None, None, "test");
        let err = EfsIamConfig::new("fs-a", "us-east-1", credentials.clone())
            .validate()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidConfig(_)));

        let err = EfsIamConfig::new("fs-1234abcd", "US East 1", credentials)
            .validate()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidConfig(_)));
    }

    #[test]
    fn query_encoding_uses_sigv4_percent_encoding() {
        assert_eq!(efs_query_encode("a b+c/="), "a%20b%2Bc%2F%3D");
    }
}
