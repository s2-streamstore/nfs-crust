use super::*;

#[test]
fn fused_put_size_is_bounded_below_pathological_efs_requests() {
    assert!(fused_put_size_viable(128 * 1024, 1024 * 1024));
    assert!(!fused_put_size_viable(128 * 1024 + 1, 1024 * 1024));
    assert!(!fused_put_size_viable(512 * 1024, 1024 * 1024));
    assert!(!fused_put_size_viable(128 * 1024, 64 * 1024));
}

fn test_config() -> NfsConfig {
    NfsConfig {
        read_chunk_size: 64 * 1024,
        write_chunk_size: 64 * 1024,
        readdir_dircount: 16 * 1024,
        readdir_maxcount: 64 * 1024,
        file_mode: 0o644,
        dir_mode: 0o755,
        session_slots: 16,
        max_request_size: 64 * 1024,
        max_response_size: 64 * 1024,
        operation_timeout: Some(Duration::from_secs(1)),
        transient_retries: 1,
        transient_retry_delay: Duration::from_millis(1),
        ..NfsConfig::new("/")
    }
}

fn put_op_reply(enc: &mut XdrEncoder, op: OpCode, status: u32) {
    enc.put_u32(op as u32);
    enc.put_u32(status);
}

fn put_exchange_id_reply(enc: &mut XdrEncoder, clientid: u64, sequenceid: u32) {
    put_op_reply(enc, OpCode::ExchangeId, status::OK);
    enc.put_u64(clientid);
    enc.put_u32(sequenceid);
    enc.put_u32(0);
    enc.put_u32(SP4_NONE);
    enc.put_u64(0);
    enc.put_opaque(b"server");
    enc.put_opaque(b"scope");
    enc.put_u32(0);
}

fn put_create_session_reply(enc: &mut XdrEncoder, sessionid: [u8; 16]) {
    put_op_reply(enc, OpCode::CreateSession, status::OK);
    enc.put_fixed_opaque(&sessionid);
    enc.put_u32(2);
    enc.put_u32(0);
    encode_channel_attrs(enc, 4096, 8192, 7);
    encode_channel_attrs(enc, 4096, 4096, 1);
}

fn put_sequence_reply(enc: &mut XdrEncoder) {
    put_op_reply(enc, OpCode::Sequence, status::OK);
    enc.put_fixed_opaque(&[1; 16]);
    enc.put_u32(2);
    enc.put_u32(3);
    enc.put_u32(4);
    enc.put_u32(5);
    enc.put_u32(0);
}

fn put_open_reply(enc: &mut XdrEncoder, stateid: StateId) {
    put_op_reply(enc, OpCode::Open, status::OK);
    stateid.encode(enc);
    enc.put_bool(true);
    enc.put_u64(1);
    enc.put_u64(2);
    enc.put_u32(0);
    Bitmap::size_attr().encode(enc);
    enc.put_u32(OPEN_DELEGATE_NONE);
}

fn put_close_reply(enc: &mut XdrEncoder, stateid: StateId) {
    put_op_reply(enc, OpCode::Close, status::OK);
    stateid.encode(enc);
}

fn put_getfh_reply(enc: &mut XdrEncoder, fh: &[u8]) {
    put_op_reply(enc, OpCode::GetFh, status::OK);
    enc.put_opaque(fh);
}

fn put_create_reply(enc: &mut XdrEncoder) {
    put_op_reply(enc, OpCode::Create, status::OK);
    put_change_info(enc);
    Bitmap::size_attr().encode(enc);
}

fn put_write_reply(enc: &mut XdrEncoder, count: u32, committed: u32, verifier: [u8; 8]) {
    put_op_reply(enc, OpCode::Write, status::OK);
    enc.put_u32(count);
    enc.put_u32(committed);
    enc.put_fixed_opaque(&verifier);
}

fn put_commit_reply(enc: &mut XdrEncoder, verifier: [u8; 8]) {
    put_op_reply(enc, OpCode::Commit, status::OK);
    enc.put_fixed_opaque(&verifier);
}

fn put_empty_readdir_reply(enc: &mut XdrEncoder, verifier: [u8; 8]) {
    put_op_reply(enc, OpCode::ReadDir, status::OK);
    enc.put_fixed_opaque(&verifier);
    enc.put_bool(false);
    enc.put_bool(true);
}

fn put_change_info(enc: &mut XdrEncoder) {
    enc.put_bool(true);
    enc.put_u64(1);
    enc.put_u64(2);
}

#[test]
fn bitmap_sets_expected_words() {
    let bitmap = Bitmap {
        words: SmallVec::from_slice(&[1 << FATTR4_SIZE, 1 << (FATTR4_MODE - 32)]),
    };
    assert_eq!(bitmap.words(), &[1 << FATTR4_SIZE, 1 << (FATTR4_MODE - 32)]);
    assert!(bitmap.contains(FATTR4_SIZE));
    assert!(bitmap.contains(FATTR4_MODE));
    assert!(!bitmap.contains(FATTR4_TYPE));
    assert_eq!(
        bitmap.attrs().collect::<Vec<_>>(),
        vec![FATTR4_SIZE, FATTR4_MODE]
    );

    let bitmap = Bitmap::size_attr();
    assert_eq!(bitmap.words(), &[1 << FATTR4_SIZE]);
    assert!(!bitmap.words.spilled());
    assert_eq!(bitmap.attrs().collect::<Vec<_>>(), vec![FATTR4_SIZE]);

    let bitmap = Bitmap::type_and_size_attrs();
    assert_eq!(bitmap.words(), &[(1 << FATTR4_TYPE) | (1 << FATTR4_SIZE)]);
    assert!(!bitmap.words.spilled());
    assert!(bitmap.contains(FATTR4_TYPE));
    assert!(bitmap.contains(FATTR4_SIZE));
    assert_eq!(
        bitmap.attrs().collect::<Vec<_>>(),
        vec![FATTR4_TYPE, FATTR4_SIZE]
    );
}

#[test]
fn bitmap_decoder_consumes_validated_words_without_losing_trailing_data() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(2);
    enc.put_u32(1 << FATTR4_TYPE);
    enc.put_u32(1 << (FATTR4_MODE - 32));
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    let bitmap = Bitmap::decode(&mut dec).unwrap();
    assert_eq!(bitmap.words(), &[1 << FATTR4_TYPE, 1 << (FATTR4_MODE - 32)]);
    assert_eq!(dec.read_u32().unwrap(), 99);

    let mut enc = XdrEncoder::new();
    enc.put_u32(3);
    enc.put_u32(1);
    enc.put_u32(2);
    enc.put_u32(4);
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    let bitmap = Bitmap::decode(&mut dec).unwrap();
    assert_eq!(bitmap.words(), &[1, 2, 4]);
    assert_eq!(dec.read_u32().unwrap(), 99);
}

#[test]
fn client_owner_id_includes_process_and_verifier() {
    let owner = ClientOwner::new();
    let mut parts = owner.ownerid.split(':');
    let pid = process::id().to_string();
    assert_eq!(parts.next(), Some("nfs-crust"));
    assert_eq!(parts.next(), Some(pid.as_str()));
    assert!(parts.next().is_some_and(|verifier| !verifier.is_empty()));
    assert_eq!(parts.next(), None);
}

#[test]
fn client_owners_are_unique_and_open_owner_counter_survives_clones() {
    let first = ClientOwner::new();
    let second = ClientOwner::new();
    assert_ne!(first.ownerid, second.ownerid);
    assert_ne!(first.verifier_value(), second.verifier_value());

    let reconnect_consumer = first.clone();
    assert_eq!(first.next_open_owner(), "nfs-crust-open-1");
    assert_eq!(reconnect_consumer.next_open_owner(), "nfs-crust-open-2");
}

#[test]
fn encodes_compound_as_minor_version_one() {
    let bytes = encode_compound("x", &[NfsOp::PutRootFh]);
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "x");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), OpCode::PutRootFh as u32);
}

#[test]
fn compound_encoded_len_matches_representative_wire_length() {
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let fh = FileHandle(Bytes::from_static(b"fh"));
    let config = test_config();
    let ops = [
        NfsOp::PutRootFh,
        NfsOp::PutFh(&fh),
        NfsOp::Lookup("dir"),
        NfsOp::SaveFh,
        NfsOp::RestoreFh,
        NfsOp::GetFh,
        NfsOp::GetAttr(Bitmap::type_and_size_attrs()),
        NfsOp::Open {
            seqid: 2,
            clientid: 99,
            owner: b"creator",
            name: "new.bin",
            file_mode: 0o640,
        },
        NfsOp::Close { seqid: 3, stateid },
        NfsOp::Read {
            stateid,
            offset: 44,
            count: 12,
        },
        NfsOp::Write {
            stateid,
            offset: 55,
            data: Bytes::from_static(b"abcde"),
        },
        NfsOp::Commit {
            offset: 55,
            count: 5,
        },
        NfsOp::Link("linked"),
        NfsOp::Remove("gone"),
        NfsOp::Verify { size: 77 },
        NfsOp::Rename {
            old_name: "old",
            new_name: "new",
        },
        NfsOp::CreateDir {
            name: "child",
            mode: 0o755,
        },
        NfsOp::ReadDir {
            cookie: 9,
            verifier: [3; 8],
            dircount: 1024,
            maxcount: 4096,
            attrs: Bitmap::type_and_size_attrs(),
        },
        NfsOp::ExchangeId {
            verifier: [4; 8],
            owner: b"client",
        },
        NfsOp::CreateSession {
            clientid: 11,
            sequenceid: 12,
            config: &config,
        },
        NfsOp::ReclaimComplete { one_fs: true },
    ];
    let bytes = encode_compound("representative", &ops);
    assert_eq!(bytes.len(), compound_encoded_len("representative", &ops));
}

#[test]
fn each_operation_encoder_writes_expected_opcode_and_length() {
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let fh = FileHandle(Bytes::from_static(b"fh"));
    let config = test_config();
    let cases = [
        (NfsOp::PutRootFh, OpCode::PutRootFh),
        (NfsOp::PutFh(&fh), OpCode::PutFh),
        (NfsOp::Lookup("dir"), OpCode::Lookup),
        (NfsOp::SaveFh, OpCode::SaveFh),
        (NfsOp::RestoreFh, OpCode::RestoreFh),
        (NfsOp::GetFh, OpCode::GetFh),
        (
            NfsOp::GetAttr(Bitmap::type_and_size_attrs()),
            OpCode::GetAttr,
        ),
        (
            NfsOp::Open {
                seqid: 2,
                clientid: 99,
                owner: b"creator",
                name: "new.bin",
                file_mode: 0o640,
            },
            OpCode::Open,
        ),
        (NfsOp::Close { seqid: 3, stateid }, OpCode::Close),
        (
            NfsOp::Read {
                stateid,
                offset: 44,
                count: 12,
            },
            OpCode::Read,
        ),
        (
            NfsOp::Write {
                stateid,
                offset: 55,
                data: Bytes::from_static(b"abcde"),
            },
            OpCode::Write,
        ),
        (
            NfsOp::Commit {
                offset: 55,
                count: 5,
            },
            OpCode::Commit,
        ),
        (NfsOp::Link("linked"), OpCode::Link),
        (NfsOp::Remove("gone"), OpCode::Remove),
        (NfsOp::Verify { size: 77 }, OpCode::Verify),
        (
            NfsOp::Rename {
                old_name: "old",
                new_name: "new",
            },
            OpCode::Rename,
        ),
        (
            NfsOp::CreateDir {
                name: "child",
                mode: 0o755,
            },
            OpCode::Create,
        ),
        (
            NfsOp::ReadDir {
                cookie: 9,
                verifier: [3; 8],
                dircount: 1024,
                maxcount: 4096,
                attrs: Bitmap::type_and_size_attrs(),
            },
            OpCode::ReadDir,
        ),
        (
            NfsOp::ExchangeId {
                verifier: [4; 8],
                owner: b"client",
            },
            OpCode::ExchangeId,
        ),
        (
            NfsOp::CreateSession {
                clientid: 11,
                sequenceid: 12,
                config: &config,
            },
            OpCode::CreateSession,
        ),
        (
            NfsOp::ReclaimComplete { one_fs: true },
            OpCode::ReclaimComplete,
        ),
    ];

    for (op, expected) in cases {
        let encoded_len = op.encoded_len();
        let bytes = encode_compound("op", std::slice::from_ref(&op));
        let mut dec = XdrDecoder::new(bytes);
        dec.skip_opaque().unwrap();
        assert_eq!(dec.read_u32().unwrap(), NFS_MINOR_VERSION);
        assert_eq!(dec.read_u32().unwrap(), 1);

        let remaining_before_op = dec.remaining();
        assert_eq!(dec.read_u32().unwrap(), expected as u32);
        dec.skip_bytes(encoded_len - 4).unwrap();
        assert_eq!(remaining_before_op, encoded_len);
        assert_eq!(dec.remaining(), 0);
    }
}

#[test]
fn encodes_session_sequence_without_building_caller_op_list() {
    let ops = [NfsOp::PutRootFh];
    let bytes = encode_compound_with_sequence(
        "seq",
        SequenceArgs {
            sessionid: [1; 16],
            sequenceid: 2,
            slotid: 3,
            highest_slotid: 4,
            cachethis: false,
        },
        &ops,
    );
    assert_eq!(bytes.len(), compound_with_sequence_encoded_len("seq", &ops));
    let mut dec = XdrDecoder::new(bytes.to_bytes());
    assert_eq!(dec.read_string().unwrap(), "seq");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 2);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Sequence as u32);
    assert_eq!(dec.read_fixed_opaque::<16>().unwrap(), [1; 16]);
    assert_eq!(dec.read_u32().unwrap(), 2);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), 4);
    assert_eq!(dec.read_u32().unwrap(), 0);
    assert_eq!(dec.read_u32().unwrap(), OpCode::PutRootFh as u32);
}

#[test]
fn direct_sequence_encoder_writes_expected_wire_layout() {
    let sequence = SequenceArgs {
        sessionid: [1; 16],
        sequenceid: 2,
        slotid: 3,
        highest_slotid: 4,
        cachethis: true,
    };

    let mut direct = XdrEncoder::new();
    encode_sequence_op(&mut direct, sequence);
    let direct = direct.freeze();

    assert_eq!(direct.len(), sequence_op_encoded_len());
    let mut dec = XdrDecoder::new(direct);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Sequence as u32);
    assert_eq!(dec.read_fixed_opaque::<16>().unwrap(), [1; 16]);
    assert_eq!(dec.read_u32().unwrap(), 2);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), 4);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.remaining(), 0);
}

#[test]
fn successful_sequence_reply_identity_and_limits_are_decoded() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("seq");
    enc.put_u32(1);
    put_sequence_reply(&mut enc);

    assert_eq!(
        decode_sequence_result(&enc.freeze(), [1; 16], 2, 3).unwrap(),
        SequenceResult {
            highest_slotid: 4,
            target_highest_slotid: 5,
            status_flags: 0,
        }
    );
}

#[test]
fn sequence_validation_rejects_unexpected_first_opcode() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("not-seq");
    enc.put_u32(1);
    enc.put_u32(u32::MAX);
    enc.put_u32(status::OK);

    let err = decode_sequence_result(&enc.freeze(), [1; 16], 2, 3).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn sequence_validation_rejects_zero_replies_and_truncated_shapes() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("seq");
    enc.put_u32(0);
    let err = decode_sequence_result(&enc.freeze(), [1; 16], 2, 3).unwrap_err();
    assert!(matches!(err, Error::Protocol(_)));

    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_u32(8);
    enc.put_bytes(b"short");
    let err = decode_sequence_result(&enc.freeze(), [1; 16], 2, 3).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));

    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("seq");
    enc.put_u32(1);
    enc.put_u32(OpCode::Sequence as u32);
    let err = decode_sequence_result(&enc.freeze(), [1; 16], 2, 3).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn sequence_result_is_skipped_as_fixed_payload() {
    let mut enc = XdrEncoder::new();
    enc.put_fixed_opaque(&[1; 16]);
    enc.put_u32(2);
    enc.put_u32(3);
    enc.put_u32(4);
    enc.put_u32(5);
    enc.put_u32(0);
    enc.put_u32(99);
    let mut dec = XdrDecoder::new(enc.freeze());

    skip_sequence_data(&mut dec).unwrap();
    assert_eq!(dec.read_u32().unwrap(), 99);

    let err = skip_sequence_data(&mut XdrDecoder::new(Bytes::from_static(&[0; 8]))).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn export_path_ops_start_from_cached_export_handle() {
    let components = vec!["dir".to_owned(), "file.bin".to_owned()];
    let fh = FileHandle(Bytes::from_static(b"export-fh"));
    let ops = export_path_ops_with_extra(&fh, &components, 0);
    assert!(!ops.spilled());

    let bytes = encode_compound("path", &ops);
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "path");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), OpCode::PutFh as u32);
    assert_eq!(&dec.read_opaque().unwrap()[..], b"export-fh");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Lookup as u32);
    assert_eq!(dec.read_string().unwrap(), "dir");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Lookup as u32);
    assert_eq!(dec.read_string().unwrap(), "file.bin");
}

#[test]
fn path_operation_lists_store_common_shapes_inline() {
    let fh = FileHandle(Bytes::from_static(b"export-fh"));
    let shallow = ["dir", "file.bin"];
    let shallow_ops = export_path_ops_with_extra(&fh, &shallow, 0);
    assert_eq!(shallow_ops.len(), 3);
    assert!(!shallow_ops.spilled());

    let deep = vec!["component"; INLINE_COMPOUND_OPS];
    let deep_ops = export_path_ops_with_extra(&fh, &deep, 0);
    assert_eq!(deep_ops.len(), INLINE_COMPOUND_OPS + 1);
    assert!(deep_ops.spilled());
}

#[test]
fn path_operation_lists_reserve_known_tail_ops() {
    let fh = FileHandle(Bytes::from_static(b"export-fh"));
    let deep = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let mut ops = export_path_ops_with_extra(&fh, &deep, 2);
    let capacity = ops.capacity();

    ops.push(NfsOp::GetFh);
    ops.push(NfsOp::ReadDir {
        cookie: 0,
        verifier: [0; 8],
        dircount: 4096,
        maxcount: 4096,
        attrs: Bitmap::type_and_size_attrs(),
    });

    assert_eq!(ops.len(), 1 + deep.len() + 2);
    assert_eq!(ops.capacity(), capacity);
}

#[test]
fn encodes_readdir_path_without_getfh_for_deferred_handle_shape() {
    let fh = FileHandle(Bytes::from_static(b"export-fh"));
    let components = ["dir"];
    let mut ops = export_path_ops_with_extra(&fh, &components, 1);
    ops.push(NfsOp::ReadDir {
        cookie: 7,
        verifier: [8; 8],
        dircount: 128,
        maxcount: 256,
        attrs: Bitmap::empty(),
    });

    let bytes = encode_compound("readdir-path", &ops);
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "readdir-path");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), OpCode::PutFh as u32);
    assert_eq!(&dec.read_opaque().unwrap()[..], b"export-fh");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Lookup as u32);
    assert_eq!(dec.read_string().unwrap(), "dir");
    assert_eq!(dec.read_u32().unwrap(), OpCode::ReadDir as u32);
    assert_eq!(dec.read_u64().unwrap(), 7);
    assert_eq!(dec.read_fixed_opaque::<8>().unwrap(), [8; 8]);
    assert_eq!(dec.read_u32().unwrap(), 128);
    assert_eq!(dec.read_u32().unwrap(), 256);
    let attrs = Bitmap::decode(&mut dec).unwrap();
    assert!(attrs.words().is_empty());
    assert_eq!(dec.remaining(), 0);
}

#[test]
fn root_path_ops_store_common_shapes_inline() {
    let shallow = ["exports", "data"];
    let ops = root_path_ops_with_extra(&shallow, 0);
    assert_eq!(ops.len(), 3);
    assert!(!ops.spilled());
}

#[test]
fn encodes_commit_range() {
    let bytes = encode_compound(
        "commit",
        &[NfsOp::Commit {
            offset: 12,
            count: 34,
        }],
    );
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "commit");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Commit as u32);
    assert_eq!(dec.read_u64().unwrap(), 12);
    assert_eq!(dec.read_u32().unwrap(), 34);
}

#[test]
fn encodes_write_as_unstable_for_final_commit_pipeline() {
    let bytes = encode_compound(
        "write",
        &[NfsOp::Write {
            stateid: StateId {
                seqid: 7,
                other: [8; 12],
            },
            offset: 55,
            data: Bytes::from_static(b"abc"),
        }],
    );
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "write");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Write as u32);
    assert_eq!(dec.read_u32().unwrap(), 7);
    assert_eq!(dec.read_fixed_opaque::<12>().unwrap(), [8; 12]);
    assert_eq!(dec.read_u64().unwrap(), 55);
    assert_eq!(dec.read_u32().unwrap(), UNSTABLE4);
    assert_eq!(&dec.read_opaque().unwrap()[..], b"abc");
}

#[test]
fn session_write_compound_borrows_the_bytes_body_as_its_middle_segment() {
    let data = Bytes::from_static(b"write-body-without-copying");
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let ops = [
        NfsOp::Write {
            stateid,
            offset: 55,
            data: data.clone(),
        },
        NfsOp::Commit {
            offset: 55,
            count: data.len() as u32,
        },
    ];
    let sequence = SequenceArgs {
        sessionid: [1; 16],
        sequenceid: 2,
        slotid: 3,
        highest_slotid: 4,
        cachethis: false,
    };

    let payload = encode_compound_with_sequence("write", sequence, &ops);
    let segments = payload.segments_for_test();
    assert_eq!(segments.len(), 3);
    assert_eq!(segments[1].as_ptr(), data.as_ptr());
    assert_eq!(segments[1].len(), data.len());
    assert_eq!(
        payload.to_bytes(),
        encode_compound_inner("write", Some(sequence), &ops)
    );
}

#[test]
fn encodes_write_and_commit_in_one_compound() {
    let opened = OpenedFile {
        fh: FileHandle(Bytes::from_static(b"fh")),
        stateid: StateId {
            seqid: 1,
            other: [2; 12],
        },
        close_seqid: 1,
    };
    let ops = write_and_commit_ops(&opened, 99, Bytes::from_static(b"xyz"));
    let bytes = encode_compound("write-commit", &ops);
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "write-commit");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), OpCode::PutFh as u32);
    assert_eq!(&dec.read_opaque().unwrap()[..], b"fh");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Write as u32);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_fixed_opaque::<12>().unwrap(), [2; 12]);
    assert_eq!(dec.read_u64().unwrap(), 99);
    assert_eq!(dec.read_u32().unwrap(), UNSTABLE4);
    assert_eq!(&dec.read_opaque().unwrap()[..], b"xyz");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Commit as u32);
    assert_eq!(dec.read_u64().unwrap(), 99);
    assert_eq!(dec.read_u32().unwrap(), 3);
}

#[test]
fn encodes_close_link_remove_publish_shape() {
    let bytes = encode_compound(
        "close-link-remove",
        &[
            NfsOp::Close {
                seqid: 3,
                stateid: StateId {
                    seqid: 4,
                    other: [5; 12],
                },
            },
            NfsOp::SaveFh,
            NfsOp::PutRootFh,
            NfsOp::Lookup("dir"),
            NfsOp::Link("target.bin"),
            NfsOp::Remove("temp.bin"),
        ],
    );
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "close-link-remove");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 6);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Close as u32);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), 4);
    assert_eq!(dec.read_fixed_opaque::<12>().unwrap(), [5; 12]);
    assert_eq!(dec.read_u32().unwrap(), OpCode::SaveFh as u32);
    assert_eq!(dec.read_u32().unwrap(), OpCode::PutRootFh as u32);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Lookup as u32);
    assert_eq!(dec.read_string().unwrap(), "dir");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Link as u32);
    assert_eq!(dec.read_string().unwrap(), "target.bin");
    assert_eq!(dec.read_u32().unwrap(), OpCode::Remove as u32);
    assert_eq!(dec.read_string().unwrap(), "temp.bin");
}

#[test]
fn encodes_rename_publish_shape() {
    let bytes = encode_compound(
        "close-rename",
        &[
            NfsOp::SaveFh,
            NfsOp::RestoreFh,
            NfsOp::Rename {
                old_name: "temp.bin",
                new_name: "target.bin",
            },
        ],
    );
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "close-rename");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 3);
    assert_eq!(dec.read_u32().unwrap(), OpCode::SaveFh as u32);
    assert_eq!(dec.read_u32().unwrap(), OpCode::RestoreFh as u32);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Rename as u32);
    assert_eq!(dec.read_string().unwrap(), "temp.bin");
    assert_eq!(dec.read_string().unwrap(), "target.bin");
}

#[test]
fn encodes_open_create_as_guarded() {
    let bytes = encode_compound(
        "open",
        &[NfsOp::Open {
            seqid: 0,
            clientid: 99,
            owner: b"owner",
            name: "file.txt",
            file_mode: 0o644,
        }],
    );
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "open");
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), OpCode::Open as u32);
    assert_eq!(dec.read_u32().unwrap(), 0);
    assert_eq!(
        dec.read_u32().unwrap(),
        OPEN4_SHARE_ACCESS_WRITE | OPEN4_SHARE_ACCESS_WANT_NO_DELEG
    );
    assert_eq!(dec.read_u32().unwrap(), OPEN4_SHARE_DENY_NONE);
    assert_eq!(dec.read_u64().unwrap(), 99);
    assert_eq!(&dec.read_opaque().unwrap()[..], b"owner");
    assert_eq!(dec.read_u32().unwrap(), OPEN_CREATE);
    assert_eq!(dec.read_u32().unwrap(), CREATE_GUARDED);
}

#[test]
fn decodes_write_stability() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(9);
    enc.put_u32(UNSTABLE4);
    enc.put_fixed_opaque(&[3; 8]);
    let mut dec = XdrDecoder::new(enc.freeze());

    let write = decode_write_data(&mut dec).unwrap();
    assert_eq!(write.count, 9);
    assert!(write.requires_commit());
    assert_eq!(write.verifier, [3; 8]);
}

#[test]
fn decodes_channel_size_limits() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(0);
    enc.put_u32(4096);
    enc.put_u32(8192);
    enc.put_u32(8192);
    enc.put_u32(64);
    enc.put_u32(3);
    enc.put_u32(0);

    let attrs = decode_channel_attrs(&mut XdrDecoder::new(enc.freeze())).unwrap();
    assert_eq!(attrs.max_request_size, 4096);
    assert_eq!(attrs.max_response_size, 8192);
    assert_eq!(attrs.maxrequests, 3);
}

#[test]
fn channel_attrs_reject_zero_usable_limits() {
    let fields = [
        (0, "ca_maxrequestsize"),
        (1, "ca_maxresponsesize"),
        (2, "ca_maxoperations"),
        (3, "ca_maxrequests"),
    ];
    for (zero_field, expected_name) in fields {
        let mut limits = [4096, 8192, 64, 3];
        limits[zero_field] = 0;
        let mut enc = XdrEncoder::new();
        enc.put_u32(0);
        enc.put_u32(limits[0]);
        enc.put_u32(limits[1]);
        enc.put_u32(8192);
        enc.put_u32(limits[2]);
        enc.put_u32(limits[3]);
        enc.put_u32(0);

        let err = decode_channel_attrs(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
        assert!(
            matches!(err, Error::Protocol(message) if message.contains(expected_name)),
            "zero {expected_name} was not rejected"
        );
    }
}

#[test]
fn fore_channel_must_fit_minimum_sequence_rpc() {
    let mut attrs = ChannelAttrs {
        max_request_size: 99,
        max_response_size: MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE,
        maxoperations: 2,
        maxrequests: 1,
    };
    assert!(matches!(
        validate_fore_channel_minimums(&attrs, 100),
        Err(Error::Protocol(_))
    ));

    attrs.max_request_size = 100;
    attrs.max_response_size = MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE - 1;
    assert!(matches!(
        validate_fore_channel_minimums(&attrs, 100),
        Err(Error::Protocol(_))
    ));
    attrs.max_response_size = MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE;
    validate_fore_channel_minimums(&attrs, 100).unwrap();
}

#[test]
fn fore_channel_requires_sequence_plus_one_operation() {
    let mut attrs = ChannelAttrs {
        max_request_size: 4096,
        max_response_size: MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE,
        maxoperations: 1,
        maxrequests: 1,
    };

    let err = validate_fore_channel_minimums(&attrs, 100).unwrap_err();
    assert!(matches!(err, Error::Protocol(message) if message.contains("ca_maxoperations 1")));

    attrs.maxoperations = 2;
    validate_fore_channel_minimums(&attrs, 100).unwrap();
}

#[test]
fn channel_attrs_skip_unused_rdma_ird_values() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(0);
    enc.put_u32(4096);
    enc.put_u32(8192);
    enc.put_u32(8192);
    enc.put_u32(64);
    enc.put_u32(3);
    enc.put_u32(3);
    enc.put_u32(10);
    enc.put_u32(11);
    enc.put_u32(12);

    let mut dec = XdrDecoder::new(enc.freeze());
    let attrs = decode_channel_attrs(&mut dec).unwrap();
    assert_eq!(attrs.max_request_size, 4096);
    assert_eq!(attrs.max_response_size, 8192);
    assert_eq!(attrs.maxrequests, 3);
    assert_eq!(dec.remaining(), 0);
}

#[test]
fn oversized_bitmap_count_is_rejected_before_allocation() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(u32::MAX);

    let err = Bitmap::decode(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn change_info_skips_fixed_tail_after_validating_atomic_flag() {
    let mut enc = XdrEncoder::new();
    put_change_info(&mut enc);
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    skip_change_info(&mut dec).unwrap();
    assert_eq!(dec.read_u32().unwrap(), 99);
    assert_eq!(dec.remaining(), 0);
}

#[test]
fn change_info_rejects_invalid_atomic_flag_before_skip() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(2);
    enc.put_u64(1);
    enc.put_u64(2);

    let err = skip_change_info(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn change_info_requires_fixed_tail_after_atomic_flag() {
    let mut enc = XdrEncoder::new();
    enc.put_bool(true);
    enc.put_u64(1);

    let err = skip_change_info(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn stateid_skip_consumes_fixed_payload() {
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let mut enc = XdrEncoder::new();
    stateid.encode(&mut enc);
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    skip_stateid(&mut dec).unwrap();
    assert_eq!(dec.read_u32().unwrap(), 99);
    assert_eq!(dec.remaining(), 0);
}

#[test]
fn open_stateid_skips_unused_attrs_set_bitmap() {
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let mut enc = XdrEncoder::new();
    stateid.encode(&mut enc);
    put_change_info(&mut enc);
    enc.put_u32(0);
    enc.put_u32(3);
    enc.put_u32(1);
    enc.put_u32(2);
    enc.put_u32(3);
    enc.put_u32(OPEN_DELEGATE_NONE);

    assert_eq!(
        decode_open_stateid(&mut XdrDecoder::new(enc.freeze())).unwrap(),
        stateid
    );
}

#[test]
fn open_stateid_consumes_result_flags_and_bitmap_len_together() {
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let mut enc = XdrEncoder::new();
    stateid.encode(&mut enc);
    put_change_info(&mut enc);
    enc.put_u32(0);

    let err = decode_open_stateid(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn open_stateid_rejects_oversized_attrs_set_bitmap_before_skip() {
    let stateid = StateId {
        seqid: 7,
        other: [8; 12],
    };
    let mut enc = XdrEncoder::new();
    stateid.encode(&mut enc);
    put_change_info(&mut enc);
    enc.put_u32(0);
    enc.put_u32(u32::MAX);

    let err = decode_open_stateid(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn state_protect_rejects_unrequested_mach_cred() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(SP4_MACH_CRED);
    enc.put_u32(2);
    enc.put_u32(1);
    enc.put_u32(2);
    enc.put_u32(1);
    enc.put_u32(3);

    let err = decode_state_protect(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)));
}

#[test]
fn oversized_compound_reply_count_is_rejected_without_large_allocation() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("oversized");
    enc.put_u32(u32::MAX);

    let err = decode_exchange_id_compound(enc.freeze()).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn setup_compound_decoder_skips_unused_reply_tag() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_opaque(&[0xff]);
    enc.put_u32(1);
    put_exchange_id_reply(&mut enc, 55, 7);

    let result = decode_exchange_id_compound(enc.freeze()).unwrap();
    assert_eq!(result.clientid, 55);
    assert_eq!(result.sequenceid, 7);
}

#[test]
fn decodes_exchange_id_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("exchange-id");
    enc.put_u32(1);
    put_exchange_id_reply(&mut enc, 99, 4);

    let result = decode_exchange_id_compound(enc.freeze()).unwrap();
    assert_eq!(result.clientid, 99);
    assert_eq!(result.sequenceid, 4);
}

#[test]
fn exchange_id_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("exchange-id");
    enc.put_u32(2);
    put_exchange_id_reply(&mut enc, 99, 4);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_exchange_id_compound(enc.freeze()).unwrap_err();
    assert!(err.to_string().contains("expected 1"));
}

#[test]
fn decodes_create_session_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("create-session");
    enc.put_u32(1);
    put_create_session_reply(&mut enc, [9; 16]);

    let result = decode_create_session_compound(enc.freeze(), 2, 1).unwrap();
    assert_eq!(result.sessionid, [9; 16]);
    assert_eq!(result.fore_attrs.max_request_size, 4096);
    assert_eq!(result.fore_attrs.max_response_size, 8192);
    assert_eq!(result.fore_attrs.maxrequests, 7);
}

#[test]
fn create_session_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("create-session");
    enc.put_u32(2);
    put_create_session_reply(&mut enc, [9; 16]);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_create_session_compound(enc.freeze(), 2, 1).unwrap_err();
    assert!(err.to_string().contains("expected 1"));
}

#[test]
fn create_session_compound_rejects_mismatched_sequence() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("create-session");
    enc.put_u32(1);
    put_create_session_reply(&mut enc, [9; 16]);

    let err = decode_create_session_compound(enc.freeze(), 3, 1).unwrap_err();
    assert!(
        matches!(err, Error::Protocol(message) if message.contains("sequence ID 2") && message.contains("request 3"))
    );
}

#[test]
fn setup_compound_decoder_preserves_error_operation() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("exchange-id");
    enc.put_u32(1);
    put_op_reply(&mut enc, OpCode::ExchangeId, NfsStatus::DELAY.code());

    let err = decode_exchange_id_compound(enc.freeze()).unwrap_err();
    assert!(err.is_nfs_error(NfsStatus::DELAY, OpCode::ExchangeId));
}

#[test]
fn decodes_no_result_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("reclaim-complete");
    enc.put_u32(2);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::ReclaimComplete, status::OK);

    decode_no_result_compound(enc.freeze(), OpCode::ReclaimComplete).unwrap();
}

#[test]
fn no_result_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("reclaim-complete");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::ReclaimComplete, status::OK);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_no_result_compound(enc.freeze(), OpCode::ReclaimComplete).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 2"));
}

#[test]
fn no_result_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("reclaim-complete");
    enc.put_u32(2);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::ReclaimComplete, NfsStatus::DELAY.code());

    let err = decode_no_result_compound(enc.freeze(), OpCode::ReclaimComplete).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::ReclaimComplete)
    );
}

#[test]
fn decodes_getfh_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("resolve");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_getfh_reply(&mut enc, b"dir-fh");

    let fh = decode_getfh_compound(enc.freeze(), OpCode::PutFh, 1).unwrap();
    assert_eq!(&fh.0[..], b"dir-fh");
}

#[test]
fn decodes_root_getfh_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("resolve-export");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutRootFh, status::OK);
    put_getfh_reply(&mut enc, b"export-fh");

    let fh = decode_getfh_compound(enc.freeze(), OpCode::PutRootFh, 0).unwrap();
    assert_eq!(&fh.0[..], b"export-fh");
}

#[test]
fn getfh_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("resolve");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_getfh_reply(&mut enc, b"dir-fh");
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_getfh_compound(enc.freeze(), OpCode::PutFh, 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 4"));
}

#[test]
fn getfh_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("resolve");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, NfsStatus::DELAY.code());

    let err = decode_getfh_compound(enc.freeze(), OpCode::PutFh, 1).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::Lookup)
    );
}

#[test]
fn decodes_create_dir_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("mkdir");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_create_reply(&mut enc);

    assert_eq!(
        decode_create_dir_compound(enc.freeze(), 1).unwrap(),
        CreateDirOutcome::Created
    );
}

#[test]
fn create_dir_compound_skips_unused_attrs_set_bitmap() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("mkdir");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Create, status::OK);
    put_change_info(&mut enc);
    enc.put_u32(3);
    enc.put_u32(1);
    enc.put_u32(2);
    enc.put_u32(3);

    assert_eq!(
        decode_create_dir_compound(enc.freeze(), 0).unwrap(),
        CreateDirOutcome::Created
    );
}

#[test]
fn create_dir_compound_rejects_oversized_attrs_set_bitmap_before_skip() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("mkdir");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Create, status::OK);
    put_change_info(&mut enc);
    enc.put_u32(u32::MAX);

    let err = decode_create_dir_compound(enc.freeze(), 0).unwrap_err();
    assert!(matches!(err.into_error(), Error::Xdr(_)));
}

#[test]
fn create_dir_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("mkdir");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_create_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_create_dir_compound(enc.freeze(), 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 4"));
}

#[test]
fn create_dir_compound_decoder_treats_exist_as_success() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::EXIST.code());
    enc.put_string("mkdir");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Create, NfsStatus::EXIST.code());

    assert_eq!(
        decode_create_dir_compound(enc.freeze(), 0).unwrap(),
        CreateDirOutcome::Exists
    );
}

#[test]
fn decodes_create_dir_handle_compound() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("mkdir-handle");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_create_reply(&mut enc);
    put_getfh_reply(&mut enc, b"created-fh");

    assert_eq!(
        decode_create_dir_handle_compound(enc.freeze(), 1).unwrap(),
        CreateDirHandleOutcome::Created(FileHandle(Bytes::from_static(b"created-fh")))
    );
}

#[test]
fn create_dir_handle_compound_treats_exist_without_getfh_as_success() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::EXIST.code());
    enc.put_string("mkdir-handle");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Create, NfsStatus::EXIST.code());

    assert_eq!(
        decode_create_dir_handle_compound(enc.freeze(), 0).unwrap(),
        CreateDirHandleOutcome::Exists
    );
}

#[test]
fn create_dir_handle_compound_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::GRACE.code());
    enc.put_string("mkdir-handle");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Create, NfsStatus::GRACE.code());

    let err = decode_create_dir_handle_compound(enc.freeze(), 0).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::GRACE));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::GRACE, OpCode::Create)
    );
}

#[test]
fn create_expected_op_consumes_fixed_header_without_losing_trailing_data() {
    let mut enc = XdrEncoder::new();
    put_op_reply(&mut enc, OpCode::Create, status::OK);
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    assert!(!decode_expected_op_or_exist(&mut dec, OpCode::Create, "CREATE").unwrap());
    assert_eq!(dec.read_u32().unwrap(), 99);

    let mut enc = XdrEncoder::new();
    enc.put_u32(OpCode::Create as u32);
    let err =
        decode_expected_op_or_exist(&mut XdrDecoder::new(enc.freeze()), OpCode::Create, "CREATE")
            .unwrap_err();
    assert!(matches!(err.into_error(), Error::Xdr(_)));
}

#[test]
fn create_dir_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::GRACE.code());
    enc.put_string("mkdir");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Create, NfsStatus::GRACE.code());

    let err = decode_create_dir_compound(enc.freeze(), 0).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::GRACE));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::GRACE, OpCode::Create)
    );
}

#[test]
fn decodes_open_file_compound_without_generic_reply_storage() {
    let stateid = StateId {
        seqid: 4,
        other: [3; 12],
    };

    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("open");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_open_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::GetFh, status::OK);
    enc.put_opaque(b"created-fh");

    let OpenOutcome::Opened((decoded_stateid, fh)) =
        decode_open_file_compound(enc.freeze(), 1).unwrap()
    else {
        panic!("open should decode as opened");
    };
    assert_eq!(decoded_stateid, stateid);
    assert_eq!(&fh.0[..], b"created-fh");
}

#[test]
fn open_file_compound_decodes_exist_as_typed_outcome() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::EXIST);
    enc.put_string("open");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Open, status::EXIST);

    assert_eq!(
        decode_open_file_compound(enc.freeze(), 0).unwrap(),
        OpenOutcome::Exists
    );
}

#[test]
fn open_file_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("open");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Open, NfsStatus::DELAY.code());

    let err = decode_open_file_compound(enc.freeze(), 0).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::Open)
    );
}

#[test]
fn open_file_compound_decoder_rejects_extra_fixed_shape_reply() {
    let stateid = StateId {
        seqid: 4,
        other: [3; 12],
    };

    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("open");
    enc.put_u32(6);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_open_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::GetFh, status::OK);
    enc.put_opaque(b"created-fh");
    put_op_reply(&mut enc, OpCode::Remove, status::OK);
    put_change_info(&mut enc);

    let err = decode_open_file_compound(enc.freeze(), 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 5"));
}

#[test]
fn decodes_close_compound_without_generic_reply_storage() {
    let stateid = StateId {
        seqid: 6,
        other: [7; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_close_reply(&mut enc, stateid);

    decode_close_compound(enc.freeze(), CloseShape::File, None).unwrap();
}

#[test]
fn close_compound_rejects_missing_close_result() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close");
    enc.put_u32(2);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_close_compound(enc.freeze(), CloseShape::File, None).unwrap_err();
    assert!(matches!(err.into_error(), Error::Protocol(message) if message.contains("CLOSE")));
}

#[test]
fn close_compound_decoder_rejects_extra_fixed_shape_reply() {
    let stateid = StateId {
        seqid: 6,
        other: [7; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_close_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_close_compound(enc.freeze(), CloseShape::File, None).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 3"));
}

#[test]
fn decodes_close_rename_compound_without_generic_reply_storage() {
    let stateid = StateId {
        seqid: 8,
        other: [9; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close-rename");
    enc.put_u32(8);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Verify, status::OK);
    put_close_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::Rename, status::OK);
    put_change_info(&mut enc);
    put_change_info(&mut enc);

    decode_close_compound(
        enc.freeze(),
        CloseShape::Rename {
            parent_lookup_count: 1,
        },
        None,
    )
    .unwrap();
}

#[test]
fn close_rename_compound_decoder_rejects_extra_fixed_shape_reply() {
    let stateid = StateId {
        seqid: 8,
        other: [9; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close-rename");
    enc.put_u32(9);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Verify, status::OK);
    put_close_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::Rename, status::OK);
    put_change_info(&mut enc);
    put_change_info(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_close_compound(
        enc.freeze(),
        CloseShape::Rename {
            parent_lookup_count: 1,
        },
        None,
    )
    .unwrap_err();
    assert!(err.into_error().to_string().contains("expected 8"));
}

#[test]
fn close_rename_compound_rejects_missing_publish_result() {
    let stateid = StateId {
        seqid: 8,
        other: [9; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close-rename");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Verify, status::OK);
    put_close_reply(&mut enc, stateid);

    let err = decode_close_compound(
        enc.freeze(),
        CloseShape::Rename {
            parent_lookup_count: 1,
        },
        None,
    )
    .unwrap_err();
    assert!(matches!(err.into_error(), Error::Protocol(message) if message.contains("publish")));
}

#[test]
fn decodes_close_link_remove_compound_without_generic_reply_storage() {
    let stateid = StateId {
        seqid: 2,
        other: [1; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close-link-remove");
    enc.put_u32(9);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Verify, status::OK);
    put_close_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::Link, status::OK);
    put_change_info(&mut enc);
    put_op_reply(&mut enc, OpCode::Remove, status::OK);
    put_change_info(&mut enc);

    decode_close_compound(
        enc.freeze(),
        CloseShape::LinkRemove {
            parent_lookup_count: 1,
        },
        None,
    )
    .unwrap();
}

#[test]
fn close_link_remove_decoder_preserves_link_conflict_operation() {
    let stateid = StateId {
        seqid: 2,
        other: [1; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::EXIST.code());
    enc.put_string("close-link-remove");
    enc.put_u32(7);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Verify, status::OK);
    put_close_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Link, NfsStatus::EXIST.code());

    let err = decode_close_compound(
        enc.freeze(),
        CloseShape::LinkRemove {
            parent_lookup_count: 0,
        },
        None,
    )
    .unwrap_err();
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::EXIST, OpCode::Link)
    );
}

fn put_close_rename_with_commit_replies(enc: &mut XdrEncoder, commit_verifier: [u8; 8]) {
    put_sequence_reply(enc);
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_commit_reply(enc, commit_verifier);
    put_op_reply(enc, OpCode::Verify, status::OK);
    put_close_reply(
        enc,
        StateId {
            seqid: 8,
            other: [9; 12],
        },
    );
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_op_reply(enc, OpCode::Lookup, status::OK);
    put_op_reply(enc, OpCode::SaveFh, status::OK);
    put_op_reply(enc, OpCode::Rename, status::OK);
    put_change_info(enc);
    put_change_info(enc);
}

#[test]
fn close_rename_compound_carries_commit_and_checks_verifier() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close-rename");
    enc.put_u32(9);
    put_close_rename_with_commit_replies(&mut enc, [5; 8]);

    decode_close_compound(
        enc.freeze(),
        CloseShape::Rename {
            parent_lookup_count: 1,
        },
        Some([5; 8]),
    )
    .unwrap();

    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("close-rename");
    enc.put_u32(9);
    put_close_rename_with_commit_replies(&mut enc, [6; 8]);

    let err = decode_close_compound(
        enc.freeze(),
        CloseShape::Rename {
            parent_lookup_count: 1,
        },
        Some([5; 8]),
    )
    .unwrap_err()
    .into_error();
    assert!(err.is_retryable());
}

#[test]
fn close_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::GRACE.code());
    enc.put_string("close");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Close, NfsStatus::GRACE.code());

    let err = decode_close_compound(enc.freeze(), CloseShape::File, None).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::GRACE));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::GRACE, OpCode::Close)
    );
}

#[test]
fn decodes_remove_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("remove");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::Remove, status::OK);
    put_change_info(&mut enc);

    assert_eq!(
        decode_remove_compound(enc.freeze(), 1).unwrap(),
        RemoveOutcome::Removed
    );
}

#[test]
fn remove_compound_decoder_treats_noent_as_missing() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::NOENT.code());
    enc.put_string("remove");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::Remove, NfsStatus::NOENT.code());

    assert_eq!(
        decode_remove_compound(enc.freeze(), 1).unwrap(),
        RemoveOutcome::Missing
    );
}

#[test]
fn remove_compound_decoder_treats_missing_parent_as_missing() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::NOENT.code());
    enc.put_string("remove");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, NfsStatus::NOENT.code());

    assert_eq!(
        decode_remove_compound(enc.freeze(), 1).unwrap(),
        RemoveOutcome::Missing
    );
}

#[test]
fn remove_compound_decoder_rejects_unknown_noent_operation() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::NOENT.code());
    enc.put_string("remove");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    enc.put_u32(99_999);
    enc.put_u32(NfsStatus::NOENT.code());

    let err = decode_remove_compound(enc.freeze(), 1).unwrap_err();
    assert!(
        err.into_error()
            .to_string()
            .contains("unknown NFS operation")
    );
}

#[test]
fn remove_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("remove");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::Remove, status::OK);
    put_change_info(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_remove_compound(enc.freeze(), 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 4"));
}

#[test]
fn remove_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::GRACE.code());
    enc.put_string("remove");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Remove, NfsStatus::GRACE.code());

    let err = decode_remove_compound(enc.freeze(), 0).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::GRACE));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::GRACE, OpCode::Remove)
    );
}

#[test]
fn failed_reply_cannot_forge_remove_at_an_earlier_position() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("close-link-remove");
    enc.put_u32(2);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::Remove, NfsStatus::DELAY.code());

    let err = decode_close_compound(
        enc.freeze(),
        CloseShape::LinkRemove {
            parent_lookup_count: 0,
        },
        None,
    )
    .unwrap_err()
    .into_error();
    assert!(matches!(err, Error::Protocol(_)));
    assert!(!err.is_nfs_operation(OpCode::Remove));
}

#[test]
fn remove_noent_only_counts_at_the_expected_reply_position() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::NOENT.code());
    enc.put_string("remove");
    enc.put_u32(2);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::Remove, NfsStatus::NOENT.code());

    let err = decode_remove_compound(enc.freeze(), 0)
        .unwrap_err()
        .into_error();
    assert!(matches!(err, Error::Protocol(_)));
    assert!(!err.is_nfs_operation(OpCode::Remove));
}

#[test]
fn guarded_create_rejects_failed_reply_with_wrong_opcode() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::EXIST.code());
    enc.put_string("open");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Remove, NfsStatus::EXIST.code());

    let err = decode_open_file_compound(enc.freeze(), 0)
        .unwrap_err()
        .into_error();
    assert!(matches!(err, Error::Protocol(_)));
}

#[test]
fn op_illegal_is_the_only_valid_mismatched_reply_opcode() {
    let mut enc = XdrEncoder::new();
    put_op_reply(&mut enc, OpCode::Illegal, status::OP_ILLEGAL);

    let err = decode_expected_op(&mut XdrDecoder::new(enc.freeze()), OpCode::Read, "READ")
        .unwrap_err()
        .into_error();
    assert!(err.is_nfs_error(NfsStatus(status::OP_ILLEGAL), OpCode::Illegal));
}

#[test]
fn decodes_read_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Read, status::OK);
    enc.put_bool(true);
    enc.put_opaque(b"abc");

    let read = decode_read_compound(enc.freeze()).unwrap();
    assert!(read.eof);
    assert_eq!(&read.data[..], b"abc");
}

fn put_read_path_size_replies(enc: &mut XdrEncoder, size: u64, eof: bool, data: &[u8]) {
    put_sequence_reply(enc);
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_op_reply(enc, OpCode::Lookup, status::OK);
    put_getfh_reply(enc, b"file-fh");
    put_op_reply(enc, OpCode::GetAttr, status::OK);
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u64(size);
    Bitmap::size_attr().encode(enc);
    enc.put_opaque(&attr_values.freeze());
    put_op_reply(enc, OpCode::Read, status::OK);
    enc.put_bool(eof);
    enc.put_opaque(data);
}

fn put_read_path_handle_replies(enc: &mut XdrEncoder, eof: bool, data: &[u8]) {
    put_sequence_reply(enc);
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_op_reply(enc, OpCode::Lookup, status::OK);
    put_getfh_reply(enc, b"file-fh");
    put_op_reply(enc, OpCode::Read, status::OK);
    enc.put_bool(eof);
    enc.put_opaque(data);
}

#[test]
fn decodes_read_path_handle_compound() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read-path-handle");
    enc.put_u32(5);
    put_read_path_handle_replies(&mut enc, false, b"abc");

    let (fh, read) = decode_read_path_handle_compound(enc.freeze(), 1).unwrap();
    assert_eq!(&fh.0[..], b"file-fh");
    assert!(!read.eof);
    assert_eq!(&read.data[..], b"abc");
}

#[test]
fn read_path_handle_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read-path-handle");
    enc.put_u32(6);
    put_read_path_handle_replies(&mut enc, true, b"abc");
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_read_path_handle_compound(enc.freeze(), 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 5"));
}

#[test]
fn read_path_handle_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("read-path-handle");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, NfsStatus::DELAY.code());

    let err = decode_read_path_handle_compound(enc.freeze(), 1).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::Lookup)
    );
}

#[test]
fn decodes_read_path_size_compound() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read-path-size");
    enc.put_u32(6);
    put_read_path_size_replies(&mut enc, 10, false, b"abc");

    let (fh, size, read) = decode_read_path_size_compound(enc.freeze(), 1).unwrap();
    assert_eq!(&fh.0[..], b"file-fh");
    assert_eq!(size, 10);
    assert!(!read.eof);
    assert_eq!(&read.data[..], b"abc");
}

#[test]
fn read_path_size_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read-path-size");
    enc.put_u32(7);
    put_read_path_size_replies(&mut enc, 10, true, b"abc");
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_read_path_size_compound(enc.freeze(), 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 6"));
}

#[test]
fn read_path_size_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("read-path-size");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, NfsStatus::DELAY.code());

    let err = decode_read_path_size_compound(enc.freeze(), 1).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::Lookup)
    );
}

#[test]
fn read_path_size_compound_requires_size_attribute() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read-path-size");
    enc.put_u32(6);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_getfh_reply(&mut enc, b"file-fh");
    put_op_reply(&mut enc, OpCode::GetAttr, status::OK);
    Bitmap::empty().encode(&mut enc);
    enc.put_opaque(&[]);
    put_op_reply(&mut enc, OpCode::Read, status::OK);
    enc.put_bool(true);
    enc.put_opaque(b"abc");

    let err = decode_read_path_size_compound(enc.freeze(), 1).unwrap_err();
    assert!(err.into_error().to_string().contains("GETATTR size result"));
}

#[test]
fn read_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("read");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Read, status::OK);
    enc.put_bool(true);
    enc.put_opaque(b"abc");
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_read_compound(enc.freeze()).unwrap_err();
    assert!(matches!(err.into_error(), Error::Protocol(message) if message.contains("expected 3")));
}

#[test]
fn read_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("read");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Read, NfsStatus::DELAY.code());

    let err = decode_read_compound(enc.freeze()).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::Read)
    );
}

#[test]
fn decodes_write_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("write");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_write_reply(&mut enc, 7, UNSTABLE4, [9; 8]);

    let write = decode_write_compound(enc.freeze()).unwrap();
    assert_eq!(write.count, 7);
    assert!(write.requires_commit());
    assert_eq!(write.verifier, [9; 8]);
}

#[test]
fn decodes_write_commit_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("write-commit");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_write_reply(&mut enc, 11, FILE_SYNC4, [2; 8]);
    put_commit_reply(&mut enc, [4; 8]);

    let (write, commit_verifier) = decode_write_commit_compound(enc.freeze()).unwrap();
    assert_eq!(write.count, 11);
    assert!(!write.requires_commit());
    assert_eq!(write.verifier, [2; 8]);
    assert_eq!(commit_verifier, [4; 8]);
}

#[test]
fn write_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::GRACE.code());
    enc.put_string("write");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Write, NfsStatus::GRACE.code());

    let err = decode_write_compound(enc.freeze()).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::GRACE));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::GRACE, OpCode::Write)
    );
}

#[test]
fn decodes_readdir_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("readdir");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_empty_readdir_reply(&mut enc, [6; 8]);

    let page = decode_readdir_compound(enc.freeze(), None).unwrap();
    assert!(page.eof);
    assert!(page.entries.is_empty());
    assert_eq!(page.verifier, [6; 8]);
}

#[test]
fn readdir_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("readdir");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_empty_readdir_reply(&mut enc, [6; 8]);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_readdir_compound(enc.freeze(), None).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 3"));
}

#[test]
fn decodes_readdir_path_compound_without_generic_reply_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("readdir-path");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::GetFh, status::OK);
    enc.put_opaque(b"dir-fh");
    put_empty_readdir_reply(&mut enc, [7; 8]);

    let (fh, page) = decode_readdir_path_compound(enc.freeze(), None, 1).unwrap();
    assert_eq!(&fh.0[..], b"dir-fh");
    assert!(page.eof);
    assert!(page.entries.is_empty());
    assert_eq!(page.verifier, [7; 8]);
}

#[test]
fn decodes_readdir_path_without_handle_compound() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("readdir-path");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_empty_readdir_reply(&mut enc, [7; 8]);

    let page = decode_readdir_path_without_handle_compound(enc.freeze(), None, 1).unwrap();
    assert!(page.eof);
    assert!(page.entries.is_empty());
    assert_eq!(page.verifier, [7; 8]);
}

#[test]
fn readdir_path_without_handle_decoder_rejects_extra_getfh_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("readdir-path");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::GetFh, status::OK);
    enc.put_opaque(b"dir-fh");
    put_empty_readdir_reply(&mut enc, [7; 8]);

    let err = decode_readdir_path_without_handle_compound(enc.freeze(), None, 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 4"));
}

#[test]
fn readdir_path_compound_decoder_rejects_extra_fixed_shape_reply() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("readdir-path");
    enc.put_u32(6);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::GetFh, status::OK);
    enc.put_opaque(b"dir-fh");
    put_empty_readdir_reply(&mut enc, [7; 8]);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);

    let err = decode_readdir_path_compound(enc.freeze(), None, 1).unwrap_err();
    assert!(err.into_error().to_string().contains("expected 5"));
}

#[test]
fn readdir_compound_decoder_preserves_transient_status() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("readdir");
    enc.put_u32(3);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::ReadDir, NfsStatus::DELAY.code());

    let err = decode_readdir_compound(enc.freeze(), None).unwrap_err();
    assert_eq!(err.transient_status(), Some(NfsStatus::DELAY));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::DELAY, OpCode::ReadDir)
    );
}

#[test]
fn oversized_channel_rdma_count_is_rejected_before_looping() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(0);
    enc.put_u32(4096);
    enc.put_u32(8192);
    enc.put_u32(8192);
    enc.put_u32(64);
    enc.put_u32(3);
    enc.put_u32(u32::MAX);

    let err = decode_channel_attrs(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn oversized_implementation_id_count_is_rejected_before_looping() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(u32::MAX);

    let err = skip_impl_id_array(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn implementation_id_array_skips_unused_values() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(1);
    enc.put_opaque(b"domain");
    enc.put_opaque(b"name");
    enc.put_u64(1);
    enc.put_u32(2);
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    skip_impl_id_array(&mut dec).unwrap();
    assert_eq!(dec.read_u32().unwrap(), 99);
    assert_eq!(dec.remaining(), 0);
}

#[test]
fn negotiated_config_clamps_payload_sizes_to_channel_limits() {
    let config = test_config();
    let fore_attrs = ChannelAttrs {
        max_request_size: 4096,
        max_response_size: 8192,
        maxoperations: 16,
        maxrequests: 4,
    };

    let config = negotiated_config(config, &fore_attrs);
    assert_eq!(config.max_request_size, 4096);
    assert_eq!(config.max_response_size, 8192);
    assert_eq!(config.write_chunk_size, 4096 - CHANNEL_PAYLOAD_RESERVE);
    assert_eq!(config.read_chunk_size, 8192 - CHANNEL_PAYLOAD_RESERVE);
    assert_eq!(config.read_granularity, 8192 - CHANNEL_PAYLOAD_RESERVE);
    assert_eq!(config.readdir_maxcount, 8192 - CHANNEL_PAYLOAD_RESERVE);
    assert_eq!(config.readdir_dircount, 8192 - CHANNEL_PAYLOAD_RESERVE);
    assert_eq!(config.max_compound_ops, 16);
}

#[test]
fn default_response_record_bound_matches_one_maximum_payload() {
    let config = NfsConfig::new("/");
    assert_eq!(
        config.max_response_size,
        config.read_chunk_size + CHANNEL_PAYLOAD_RESERVE
    );
    assert_eq!(config.readdir_maxcount, config.read_chunk_size);
}

#[test]
fn write_commit_count_uses_requested_write_len_until_count4_overflows() {
    assert_eq!(commit_count(1), 1);
    assert_eq!(commit_count(u64::from(u32::MAX)), u32::MAX);
    assert_eq!(commit_count(u64::from(u32::MAX) + 1), 0);
}

#[test]
fn bounded_readdir_counts_use_list_hint_without_exceeding_config() {
    let config = NfsConfig {
        readdir_dircount: 64 * 1024,
        readdir_maxcount: 1024 * 1024,
        max_response_size: 2 * 1024 * 1024,
        ..test_config()
    };

    assert_eq!(
        bounded_readdir_counts(&config, None),
        (64 * 1024, 1024 * 1024)
    );
    assert_eq!(
        bounded_readdir_counts(&config, Some(1)),
        (READDIR_MIN_HINT_BYTES, READDIR_MIN_HINT_BYTES)
    );
    assert_eq!(
        bounded_readdir_counts(&config, Some(100)),
        (
            100 * READDIR_ENTRY_BYTE_HINT as u32,
            100 * READDIR_ENTRY_BYTE_HINT as u32
        )
    );
    assert_eq!(
        bounded_readdir_counts(&config, Some(usize::MAX)),
        (64 * 1024, 1024 * 1024)
    );
}

#[test]
fn readdir_entries_capacity_uses_response_size_hint_with_cap() {
    assert_eq!(readdir_entries_capacity(0, None), 0);
    assert_eq!(
        readdir_entries_capacity(READDIR_ENTRY_BYTE_HINT - 1, None),
        0
    );
    assert_eq!(
        readdir_entries_capacity(READDIR_ENTRY_BYTE_HINT * 3, None),
        3
    );
    assert_eq!(
        readdir_entries_capacity(
            READDIR_ENTRY_BYTE_HINT * (READDIR_DECODE_PREALLOC_LIMIT + 1),
            None,
        ),
        READDIR_DECODE_PREALLOC_LIMIT
    );
    assert_eq!(
        readdir_entries_capacity(READDIR_ENTRY_BYTE_HINT * 3, Some(2)),
        2
    );
}

#[test]
fn decodes_requested_file_attributes() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u32(NF4REG);
    attr_values.put_u64(123);

    let mut enc = XdrEncoder::new();
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values.freeze());

    let mut dec = XdrDecoder::new(enc.freeze());
    let attrs = decode_fattr(&mut dec).unwrap();
    assert_eq!(attrs.file_type, Some(FileType::Regular));
    assert_eq!(attrs.size, Some(123));
}

#[test]
fn decodes_common_size_only_file_attributes() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u64(123);

    let mut enc = XdrEncoder::new();
    Bitmap::size_attr().encode(&mut enc);
    enc.put_opaque(&attr_values.freeze());

    let mut dec = XdrDecoder::new(enc.freeze());
    let attrs = decode_fattr(&mut dec).unwrap();
    assert_eq!(attrs.file_type, None);
    assert_eq!(attrs.size, Some(123));
}

#[test]
fn decodes_common_file_attributes_in_place_and_skips_declared_tail() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u64(123);
    attr_values.put_u32(99);

    let mut enc = XdrEncoder::new();
    Bitmap::size_attr().encode(&mut enc);
    enc.put_opaque(&attr_values.freeze());
    enc.put_u32(55);

    let mut dec = XdrDecoder::new(enc.freeze());
    let attrs = decode_fattr(&mut dec).unwrap();
    assert_eq!(attrs.file_type, None);
    assert_eq!(attrs.size, Some(123));
    assert_eq!(dec.read_u32().unwrap(), 55);
}

#[test]
fn common_type_attribute_decoder_skips_declared_tail() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u32(NF4DIR);
    attr_values.put_u64(123);
    attr_values.put_u32(77);

    let mut enc = XdrEncoder::new();
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values.freeze());
    enc.put_u32(55);

    let mut dec = XdrDecoder::new(enc.freeze());
    let attrs = decode_fattr(&mut dec).unwrap();
    assert_eq!(attrs.file_type, Some(FileType::Directory));
    assert_eq!(attrs.size, Some(123));
    assert_eq!(dec.read_u32().unwrap(), 55);
}

#[test]
fn generic_file_attribute_decoder_decodes_in_place_and_skips_declared_tail() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u32(NF4REG);
    attr_values.put_u64(123);
    attr_values.put_u32(0o644);
    attr_values.put_u32(77);

    let bitmap = Bitmap {
        words: SmallVec::from_slice(&[
            (1 << FATTR4_TYPE) | FATTR4_SIZE_WORD,
            1 << (FATTR4_MODE - 32),
        ]),
    };
    let mut enc = XdrEncoder::new();
    bitmap.encode(&mut enc);
    enc.put_opaque(&attr_values.freeze());
    enc.put_u32(55);

    let mut dec = XdrDecoder::new(enc.freeze());
    let attrs = decode_fattr(&mut dec).unwrap();

    assert_eq!(attrs.file_type, Some(FileType::Regular));
    assert_eq!(attrs.size, Some(123));
    assert_eq!(dec.read_u32().unwrap(), 55);
}

#[test]
fn common_file_attribute_decoder_rejects_truncated_payload() {
    let mut enc = XdrEncoder::new();
    Bitmap::size_attr().encode(&mut enc);
    enc.put_opaque(&[0, 0, 0, 1]);

    let err = decode_fattr(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn common_file_attribute_decoder_rejects_truncated_single_word_bitmap() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(1);

    let err = decode_fattr(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn common_type_attribute_decoder_rejects_truncated_payloads() {
    let mut enc = XdrEncoder::new();
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&[0; 8]);

    let err = decode_fattr(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn skipped_file_attributes_skip_bitmap_without_decoding_words() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(3);
    enc.put_u32(1);
    enc.put_u32(2);
    enc.put_u32(3);
    enc.put_opaque(b"ignored-attrs");
    enc.put_u32(99);

    let mut dec = XdrDecoder::new(enc.freeze());
    skip_fattr(&mut dec).unwrap();
    assert_eq!(dec.read_u32().unwrap(), 99);
}

#[test]
fn skipped_file_attributes_reject_oversized_bitmap_before_skip() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(u32::MAX);

    let err = skip_fattr(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Xdr(_)));
}

#[test]
fn decodes_readdir_linked_list() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u32(NF4DIR);
    attr_values.put_u64(0);
    let attr_values = attr_values.freeze();

    let mut enc = XdrEncoder::new();
    enc.put_fixed_opaque(&[7; 8]);
    enc.put_bool(true);
    enc.put_u64(40);
    enc.put_string(".");
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values);
    enc.put_bool(true);
    enc.put_u64(41);
    enc.put_string("..");
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values);
    enc.put_bool(true);
    enc.put_u64(42);
    enc.put_string("dir");
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values);
    enc.put_bool(false);
    enc.put_bool(true);

    let mut dec = XdrDecoder::new(enc.freeze());
    let page = decode_readdir(&mut dec, None).unwrap();
    assert!(page.eof);
    assert_eq!(page.last_cookie, 42);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "dir");
}

#[test]
fn readdir_decoder_stops_at_entry_limit() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u32(NF4REG);
    attr_values.put_u64(123);
    let attr_values = attr_values.freeze();

    let mut enc = XdrEncoder::new();
    enc.put_fixed_opaque(&[7; 8]);
    enc.put_bool(true);
    enc.put_u64(42);
    enc.put_string("first");
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values);
    enc.put_bool(true);
    enc.put_u64(43);
    enc.put_string("second");
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values);
    enc.put_bool(false);
    enc.put_bool(true);

    let mut dec = XdrDecoder::new(enc.freeze());
    let page = decode_readdir(&mut dec, Some(1)).unwrap();

    assert!(!page.eof);
    assert_eq!(page.last_cookie, 42);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "first");
}

#[test]
fn readdir_skips_dot_entry_attrs_without_decoding_them() {
    let mut attr_values = XdrEncoder::new();
    attr_values.put_u32(NF4DIR);
    attr_values.put_u64(0);
    let attr_values = attr_values.freeze();

    let mut enc = XdrEncoder::new();
    enc.put_fixed_opaque(&[7; 8]);
    enc.put_bool(true);
    enc.put_u64(40);
    enc.put_string(".");
    enc.put_u32(1);
    enc.put_u32(1 << 30);
    enc.put_opaque(&[]);
    enc.put_bool(true);
    enc.put_u64(41);
    enc.put_string("dir");
    Bitmap::type_and_size_attrs().encode(&mut enc);
    enc.put_opaque(&attr_values);
    enc.put_bool(false);
    enc.put_bool(true);

    let page = decode_readdir(&mut XdrDecoder::new(enc.freeze()), None).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "dir");
    assert_eq!(page.last_cookie, 41);
}

fn put_fused_replies(
    enc: &mut XdrEncoder,
    write_count: u32,
    write_verifier: [u8; 8],
    commit_verifier: [u8; 8],
    verify_status: u32,
) {
    put_sequence_reply(enc);
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_op_reply(enc, OpCode::Lookup, status::OK);
    put_op_reply(enc, OpCode::SaveFh, status::OK);
    put_open_reply(
        enc,
        StateId {
            seqid: 3,
            other: [4; 12],
        },
    );
    put_getfh_reply(enc, b"temp-fh");
    put_write_reply(enc, write_count, UNSTABLE4, write_verifier);
    put_commit_reply(enc, commit_verifier);
    put_op_reply(enc, OpCode::Verify, verify_status);
}

fn put_fused_rename_tail(enc: &mut XdrEncoder) {
    put_op_reply(enc, OpCode::RestoreFh, status::OK);
    put_op_reply(enc, OpCode::Rename, status::OK);
    put_change_info(enc);
    put_change_info(enc);
}

/// Replies for a create-new fused put with one parent lookup, up to and
/// including the LINK reply with `link_status`.
fn put_fused_link_replies_through_link(enc: &mut XdrEncoder, link_status: u32) {
    put_sequence_reply(enc);
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_op_reply(enc, OpCode::Lookup, status::OK);
    put_open_reply(
        enc,
        StateId {
            seqid: 3,
            other: [4; 12],
        },
    );
    put_getfh_reply(enc, b"temp-fh");
    put_write_reply(enc, 6, UNSTABLE4, [9; 8]);
    put_commit_reply(enc, [9; 8]);
    put_op_reply(enc, OpCode::Verify, status::OK);
    put_op_reply(enc, OpCode::SaveFh, status::OK);
    put_op_reply(enc, OpCode::PutFh, status::OK);
    put_op_reply(enc, OpCode::Lookup, status::OK);
    put_op_reply(enc, OpCode::Link, link_status);
}

#[test]
fn decodes_fused_put_compound() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("put-fused");
    enc.put_u32(11);
    put_fused_replies(&mut enc, 6, [9; 8], [9; 8], status::OK);
    put_fused_rename_tail(&mut enc);

    let OpenOutcome::Opened((stateid, fh, write)) =
        decode_fused_put_compound(enc.freeze(), 1, false).unwrap()
    else {
        panic!("fused put should decode as opened");
    };
    assert_eq!(
        stateid,
        StateId {
            seqid: 3,
            other: [4; 12],
        }
    );
    assert_eq!(&fh.0[..], b"temp-fh");
    assert_eq!(write.count, 6);
}

#[test]
fn decodes_fused_put_link_remove_compound() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("put-fused");
    enc.put_u32(13);
    put_fused_link_replies_through_link(&mut enc, status::OK);
    put_change_info(&mut enc);
    put_op_reply(&mut enc, OpCode::Remove, status::OK);
    put_change_info(&mut enc);

    let OpenOutcome::Opened((_, fh, write)) =
        decode_fused_put_compound(enc.freeze(), 1, true).unwrap()
    else {
        panic!("fused put should decode as opened");
    };
    assert_eq!(&fh.0[..], b"temp-fh");
    assert_eq!(write.count, 6);
}

#[test]
fn fused_put_open_exist_decodes_as_typed_outcome() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::EXIST);
    enc.put_string("put-fused");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::Open, status::EXIST);

    assert!(matches!(
        decode_fused_put_compound(enc.freeze(), 1, false).unwrap(),
        OpenOutcome::Exists
    ));
}

#[test]
fn fused_put_link_conflict_preserves_link_operation() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::EXIST);
    enc.put_string("put-fused");
    enc.put_u32(12);
    put_fused_link_replies_through_link(&mut enc, status::EXIST);

    let err = decode_fused_put_compound(enc.freeze(), 1, true).unwrap_err();
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::EXIST, OpCode::Link)
    );
}

#[test]
fn fused_put_rejects_changed_write_verifier() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("put-fused");
    enc.put_u32(11);
    put_fused_replies(&mut enc, 6, [9; 8], [8; 8], status::OK);
    put_fused_rename_tail(&mut enc);

    let err = decode_fused_put_compound(enc.freeze(), 1, false)
        .unwrap_err()
        .into_error();
    assert!(err.is_retryable());
}

#[test]
fn fused_put_short_write_stops_at_verify() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::NOT_SAME);
    enc.put_string("put-fused");
    enc.put_u32(9);
    put_fused_replies(&mut enc, 4, [9; 8], [9; 8], status::NOT_SAME);

    let err = decode_fused_put_compound(enc.freeze(), 1, false).unwrap_err();
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus(status::NOT_SAME), OpCode::Verify)
    );
}

#[test]
fn fused_put_rejects_missing_publish_result() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("put-fused");
    enc.put_u32(9);
    put_fused_replies(&mut enc, 6, [9; 8], [9; 8], status::OK);

    let err = decode_fused_put_compound(enc.freeze(), 1, false).unwrap_err();
    assert!(matches!(err.into_error(), Error::Protocol(message) if message.contains("publish")));
}

#[tokio::test]
async fn exhausted_slot_table_returns_retryable_connection_loss() {
    let table = SlotTable::new(1);
    {
        let mut slot = table.acquire().await.unwrap();
        assert_eq!(table.available.available_permits(), 0);
        slot.mark_unusable();
    }
    assert_eq!(table.available.available_permits(), 0);

    let err = match table.acquire().await {
        Ok(_) => panic!("acquired an unusable session slot"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::ConnectionLost(_)));
    assert!(err.is_retryable());
}

#[tokio::test]
async fn slot_table_rotates_probe_start() {
    let table = SlotTable::new(3);

    let first = table.acquire().await.unwrap();
    assert_eq!(first.id, 0);

    let second = table.acquire().await.unwrap();
    assert_eq!(second.id, 1);

    let third = table.acquire().await.unwrap();
    assert_eq!(third.id, 2);

    drop(first);
    drop(second);
    drop(third);

    let fourth = table.acquire().await.unwrap();
    assert_eq!(fourth.id, 0);
}

#[tokio::test]
async fn slot_table_waits_for_live_slot_permit() {
    let table = Arc::new(SlotTable::new(1));
    let first = table.acquire().await.unwrap();
    let waiter_table = Arc::clone(&table);
    let waiter = tokio::spawn(async move {
        let slot = waiter_table.acquire().await.unwrap();
        slot.id
    });

    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    drop(first);
    assert_eq!(waiter.await.unwrap(), 0);
    assert_eq!(table.available.available_permits(), 1);
}

#[tokio::test]
async fn dynamically_disabled_permits_wake_after_slot_unlock() {
    let table = Arc::new(SlotTable::new(4));
    table.update_limits(3, 0);
    let first = table.acquire().await.unwrap();
    let waiter_table = Arc::clone(&table);
    let waiter = tokio::spawn(async move {
        waiter_table
            .acquire_for_request()
            .await
            .map(|(slot, _)| slot.id)
    });

    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(first);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn dropping_an_outstanding_request_retires_its_slot() {
    let table = SlotTable::new(1);
    {
        let mut slot = table.acquire().await.unwrap();
        slot.mark_request_outstanding();
    }

    let err = match table.acquire().await {
        Ok(_) => panic!("cancelled request slot was reused"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::ConnectionLost(_)));
}

#[tokio::test]
async fn matching_reply_keeps_slot_live_and_sequence_wraps_to_zero() {
    let table = SlotTable::new(1);
    {
        let mut slot = table.acquire().await.unwrap();
        slot.sequenceid = u32::MAX;
        slot.mark_request_outstanding();
        slot.advance_sequence();
        slot.disarm_request();
    }

    let slot = table.acquire().await.unwrap();
    assert_eq!(slot.sequenceid, 0);
}

#[tokio::test]
async fn slot_table_obeys_dynamic_target_limit() {
    let table = SlotTable::new(4);
    table.update_limits(3, 0);

    let first = table.acquire().await.unwrap();
    assert_eq!(first.id, 0);
    assert_eq!(table.request_highest_slotid(first.id).await, Some(0));
    drop(first);

    table.update_limits(3, 2);
    let first = table.acquire().await.unwrap();
    let second = table.acquire().await.unwrap();
    let third = table.acquire().await.unwrap();
    assert_eq!([first.id, second.id, third.id], [0, 1, 2]);
    assert_eq!(table.request_highest_slotid(third.id).await, Some(2));
}

#[tokio::test]
async fn slot_selected_during_target_shrink_is_reacquired_without_deadlock() {
    let table = SlotTable::new(4);
    let slot0 = table.acquire().await.unwrap();
    let slot1 = table.acquire().await.unwrap();
    let slot2 = table.acquire().await.unwrap();
    let stale_high_slot = table.acquire().await.unwrap();
    assert_eq!(stale_high_slot.id, 3);

    table.update_limits(0, 0);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            table.request_highest_slotid(stale_high_slot.id)
        )
        .await
        .unwrap(),
        None
    );

    drop(stale_high_slot);
    drop(slot0);
    drop(slot1);
    drop(slot2);
    let (replacement, highest_slotid) =
        tokio::time::timeout(Duration::from_secs(1), table.acquire_for_request())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(replacement.id, 0);
    assert_eq!(highest_slotid, 0);
}

#[test]
fn sequence_validation_rejects_mismatched_request_identity() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("seq");
    enc.put_u32(1);
    put_sequence_reply(&mut enc);
    let response = enc.freeze();

    for (sessionid, sequenceid, slotid) in [([9; 16], 2, 3), ([1; 16], 9, 3), ([1; 16], 2, 9)] {
        let err = decode_sequence_result(&response, sessionid, sequenceid, slotid).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }
}

#[test]
fn sequence_status_flags_reject_state_loss_and_unsupported_state() {
    assert!(validate_sequence_status_flags(SEQ4_STATUS_CB_PATH_DOWN, true).is_ok());
    assert!(validate_sequence_status_flags(SEQ4_STATUS_RESTART_RECLAIM_NEEDED, false).is_ok());
    assert!(matches!(
        validate_sequence_status_flags(SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED, true),
        Err(Error::ConnectionLost(_))
    ));
    assert!(matches!(
        validate_sequence_status_flags(SEQ4_STATUS_RESTART_RECLAIM_NEEDED, true),
        Err(Error::ConnectionLost(_))
    ));
    assert!(matches!(
        validate_sequence_status_flags(SEQ4_STATUS_LEASE_MOVED, true),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        validate_sequence_status_flags(1 << 31, true),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn revoked_state_rotates_client_incarnation_exactly_once() {
    let owner = ClientOwner::new();
    let old_verifier = owner.verifier_value();
    let ownerid = owner.ownerid.clone();
    assert_eq!(owner.next_open_owner(), "nfs-crust-open-1");

    let first_session_flags = AtomicU32::new(0);
    assert!(latch_fatal_sequence_flags(
        &first_session_flags,
        &owner,
        old_verifier,
        SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED,
    ));
    let new_verifier = owner.verifier_value();
    assert_ne!(new_verifier, old_verifier);

    assert!(!latch_fatal_sequence_flags(
        &first_session_flags,
        &owner,
        old_verifier,
        SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED,
    ));
    let concurrent_old_session_flags = AtomicU32::new(0);
    assert!(!latch_fatal_sequence_flags(
        &concurrent_old_session_flags,
        &owner,
        old_verifier,
        SEQ4_STATUS_ADMIN_STATE_REVOKED,
    ));
    assert_eq!(owner.verifier_value(), new_verifier);
    assert_eq!(owner.ownerid, ownerid);
    assert_eq!(owner.next_open_owner(), "nfs-crust-open-2");
}

#[test]
fn restart_reclaim_does_not_rotate_client_incarnation() {
    let owner = ClientOwner::new();
    let verifier = owner.verifier_value();
    let flags = AtomicU32::new(0);
    assert!(!latch_fatal_sequence_flags(
        &flags,
        &owner,
        verifier,
        SEQ4_STATUS_RESTART_RECLAIM_NEEDED,
    ));
    assert_eq!(owner.verifier_value(), verifier);
}

#[test]
fn reconnect_exchange_uses_rotated_client_verifier() {
    let owner = ClientOwner::new();
    let old_verifier = owner.verifier_value();
    let flags = AtomicU32::new(0);
    assert!(latch_fatal_sequence_flags(
        &flags,
        &owner,
        old_verifier,
        SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED,
    ));
    let reconnect_verifier = owner.verifier_value();

    let bytes = encode_compound(
        "exchange-id",
        &[NfsOp::ExchangeId {
            verifier: reconnect_verifier.to_be_bytes(),
            owner: owner.ownerid.as_bytes(),
        }],
    );
    let mut dec = XdrDecoder::new(bytes);
    assert_eq!(dec.read_string().unwrap(), "exchange-id");
    assert_eq!(dec.read_u32().unwrap(), NFS_MINOR_VERSION);
    assert_eq!(dec.read_u32().unwrap(), 1);
    assert_eq!(dec.read_u32().unwrap(), OpCode::ExchangeId as u32);
    assert_eq!(
        dec.read_bytes::<8>().unwrap(),
        reconnect_verifier.to_be_bytes()
    );
    assert_ne!(reconnect_verifier, old_verifier);
}

#[test]
fn all_session_compounds_enforce_negotiated_operation_and_request_limits() {
    let ops = [NfsOp::PutRootFh];
    let encoded_len = compound_with_sequence_encoded_len("limited", &ops);
    let encoded_call_len = encoded_len + 128;
    let exact_channel_size = encoded_call_len as u32;

    validate_compound_request_limits("limited", &ops, 2, exact_channel_size, encoded_call_len)
        .unwrap();
    let op_err =
        validate_compound_request_limits("limited", &ops, 1, exact_channel_size, encoded_call_len)
            .unwrap_err();
    assert!(matches!(op_err, Error::Protocol(message) if message.contains("operations")));
    let size_err = validate_compound_request_limits(
        "limited",
        &ops,
        2,
        exact_channel_size - 1,
        encoded_call_len,
    )
    .unwrap_err();
    assert!(matches!(size_err, Error::Protocol(message) if message.contains("RPC request")));

    assert_eq!(saturating_encoded_len([usize::MAX - 1, 8]), usize::MAX);
    let overflow_err =
        validate_compound_request_limits("limited", &ops, 2, u32::MAX, usize::MAX).unwrap_err();
    assert!(matches!(overflow_err, Error::Protocol(_)));
}

#[test]
fn decoded_filehandles_are_bounded_and_detached_from_response_storage() {
    let mut enc = XdrEncoder::new();
    enc.put_opaque(b"small-fh");
    let encoded = enc.freeze();
    let response_start = encoded.as_ptr() as usize;
    let response_end = response_start + encoded.len();
    let fh = decode_file_handle(&mut XdrDecoder::new(encoded)).unwrap();
    let handle_ptr = fh.0.as_ptr() as usize;
    assert!(handle_ptr < response_start || handle_ptr >= response_end);

    let mut enc = XdrEncoder::new();
    enc.put_opaque(&[7; NFS4_FHSIZE + 1]);
    let err = decode_file_handle(&mut XdrDecoder::new(enc.freeze())).unwrap_err();
    assert!(matches!(err, Error::Protocol(message) if message.contains("filehandle")));
}

#[test]
fn transient_retry_stops_after_successful_open() {
    let stateid = StateId {
        seqid: 8,
        other: [9; 12],
    };
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::DELAY.code());
    enc.put_string("open");
    enc.put_u32(4);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_open_reply(&mut enc, stateid);
    put_op_reply(&mut enc, OpCode::GetFh, NfsStatus::DELAY.code());

    let err = decode_open_file_compound(enc.freeze(), 0).unwrap_err();
    let ops = [
        NfsOp::PutRootFh,
        NfsOp::Open {
            seqid: 0,
            clientid: 1,
            owner: b"owner",
            name: "temp",
            file_mode: 0o600,
        },
        NfsOp::GetFh,
    ];
    assert!(err.completed_operation(&ops, OpCode::Open));
    assert!(!transient_compound_retry_is_safe(&ops, &err));
}

#[test]
fn transient_retry_budget_is_shared_and_zero_disables_phase_resume() {
    let mut attempts = 0;
    assert_eq!(claim_transient_retry(&mut attempts, 0), None);
    assert_eq!(attempts, 0);

    assert_eq!(claim_transient_retry(&mut attempts, 2), Some(1));
    assert_eq!(claim_transient_retry(&mut attempts, 2), Some(2));
    assert_eq!(claim_transient_retry(&mut attempts, 2), None);
    assert_eq!(attempts, 2);
}

#[test]
fn transient_retry_delay_saturates_instead_of_panicking() {
    assert_eq!(
        transient_retry_delay(Duration::from_millis(25), 3),
        Duration::from_millis(75)
    );
    assert_eq!(
        transient_retry_delay(Duration::MAX, u32::MAX),
        Duration::MAX
    );
}

#[test]
fn resumed_link_publish_preserves_success_when_only_cleanup_is_delayed() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(NfsStatus::GRACE.code());
    enc.put_string("publish-link-remove");
    enc.put_u32(7);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::Link, status::OK);
    put_change_info(&mut enc);
    put_op_reply(&mut enc, OpCode::Remove, NfsStatus::GRACE.code());

    let err = decode_link_remove_publish_compound(enc.freeze(), 1).unwrap_err();
    let source_fh = FileHandle(Bytes::from_static(b"source"));
    let export_root = FileHandle(Bytes::from_static(b"root"));
    let ops = [
        NfsOp::PutFh(&source_fh),
        NfsOp::SaveFh,
        NfsOp::PutFh(&export_root),
        NfsOp::Lookup("parent"),
        NfsOp::Link("dest"),
        NfsOp::Remove("temp"),
    ];
    assert!(err.completed_operation(&ops, OpCode::Link));
    assert!(!transient_compound_retry_is_safe(&ops, &err));
    assert!(
        err.into_error()
            .is_nfs_error(NfsStatus::GRACE, OpCode::Remove)
    );
}

#[test]
fn resumed_link_publish_fits_sixteen_op_limit_for_deep_parent() {
    let source_fh = FileHandle(Bytes::from_static(b"source"));
    let export_root = FileHandle(Bytes::from_static(b"root"));
    let parent = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    let ops = link_remove_publish_ops(
        &source_fh,
        &export_root,
        &parent,
        "destination",
        "temporary",
    );

    assert_eq!(ops.len() + 1, 16, "SEQUENCE counts toward the limit");
    validate_compound_request_limits(
        "publish-link-remove",
        &ops,
        16,
        u32::MAX,
        compound_with_sequence_encoded_len("publish-link-remove", &ops),
    )
    .unwrap();
}

#[test]
fn resumed_rename_publish_decodes_without_repeating_close() {
    let mut enc = XdrEncoder::new();
    enc.put_u32(status::OK);
    enc.put_string("publish-rename");
    enc.put_u32(5);
    put_sequence_reply(&mut enc);
    put_op_reply(&mut enc, OpCode::PutFh, status::OK);
    put_op_reply(&mut enc, OpCode::Lookup, status::OK);
    put_op_reply(&mut enc, OpCode::SaveFh, status::OK);
    put_op_reply(&mut enc, OpCode::Rename, status::OK);
    put_change_info(&mut enc);
    put_change_info(&mut enc);

    decode_rename_publish_compound(enc.freeze(), 1).unwrap();
}
