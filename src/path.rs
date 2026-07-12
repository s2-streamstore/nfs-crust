use crate::error::Error;

use smallvec::SmallVec;

const INLINE_PATH_COMPONENTS: usize = 4;
pub(crate) const TEMP_FILE_PREFIX: &str = ".nfs-crust-tmp-";

type PathComponents<'a> = SmallVec<[&'a str; INLINE_PATH_COMPONENTS]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NfsPath<'a> {
    components: PathComponents<'a>,
}

impl<'a> NfsPath<'a> {
    pub(crate) fn file(path: &'a str) -> Result<Self, Error> {
        let parsed = parse_components(path)?;
        if parsed.components().is_empty() {
            return Err(Error::InvalidPath("file path must not be empty".to_owned()));
        }
        validate_user_components(parsed.components())?;
        Ok(parsed)
    }

    pub(crate) fn directory(path: &'a str) -> Result<Self, Error> {
        let parsed = parse_components(path)?;
        validate_user_components(parsed.components())?;
        Ok(parsed)
    }

    pub(crate) fn parent_and_name(&self) -> Result<(&[&'a str], &'a str), Error> {
        let Some((name, parent)) = self.components().split_last() else {
            return Err(Error::InvalidPath("file path must not be empty".to_owned()));
        };
        Ok((parent, *name))
    }

    pub(crate) fn components(&self) -> &[&'a str] {
        &self.components
    }
}

pub(crate) fn parse_export(export: &str) -> Result<NfsPath<'_>, Error> {
    parse_components(export)
}

fn validate_user_components(components: &[&str]) -> Result<(), Error> {
    if let Some(component) = components
        .iter()
        .find(|component| component.starts_with(TEMP_FILE_PREFIX))
    {
        return Err(Error::InvalidPath(format!(
            "path component {component:?} uses the reserved nfs-crust temporary-file namespace"
        )));
    }
    Ok(())
}

fn parse_components(value: &str) -> Result<NfsPath<'_>, Error> {
    let bytes = value.as_bytes();
    let mut start = 0;
    while start < bytes.len() && bytes[start] == b'/' {
        start += 1;
    }

    let mut end = bytes.len();
    while end > start && bytes[end - 1] == b'/' {
        end -= 1;
    }

    if start == end {
        return Ok(NfsPath {
            components: PathComponents::new(),
        });
    }

    let mut components = PathComponents::new();
    let mut component_start = start;
    for (offset, byte) in bytes[start..end].iter().enumerate() {
        let index = start + offset;
        match *byte {
            0 => {
                return Err(Error::InvalidPath(
                    "NUL bytes are not valid in paths".to_owned(),
                ));
            }
            b'/' => {
                if index == component_start {
                    return Err(Error::InvalidPath(
                        "empty path components are not valid paths".to_owned(),
                    ));
                }
                push_component(value, component_start, index, &mut components)?;
                component_start = index + 1;
            }
            _ => {}
        }
    }
    push_component(value, component_start, end, &mut components)?;

    Ok(NfsPath { components })
}

fn push_component<'a>(
    path: &'a str,
    start: usize,
    end: usize,
    components: &mut PathComponents<'a>,
) -> Result<(), Error> {
    let component = &path[start..end];
    if component == "." || component == ".." {
        return Err(Error::InvalidPath(format!(
            "reserved path component {component:?} is not valid in paths"
        )));
    }
    components.push(component);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_reject_traversal_and_empty_components() {
        assert!(NfsPath::file("../x").is_err());
        assert!(NfsPath::file("a/../x").is_err());
        assert!(NfsPath::file("a//x").is_err());
        assert!(NfsPath::file("a/\0/x").is_err());
        assert!(NfsPath::file("/").is_err());
    }

    #[test]
    fn user_paths_reserve_the_internal_temporary_namespace() {
        assert!(NfsPath::file(".nfs-crust-tmp-01ARZ3NDEKTSV4RRFFQ69G5FAV").is_err());
        assert!(NfsPath::file("data/.nfs-crust-tmp-custom/file").is_err());
        assert!(NfsPath::directory("data/.nfs-crust-tmp-custom").is_err());

        // An export is server configuration, not a user path inside the
        // export, and may legitimately contain the reserved-looking name.
        assert!(parse_export("/.nfs-crust-tmp-export").is_ok());
    }

    #[test]
    fn parser_trims_boundary_slashes_before_component_push() {
        let path = NfsPath::file("///a/b///").unwrap();
        assert_eq!(path.components(), &["a", "b"]);

        let err = NfsPath::file("///a//b///").unwrap_err();
        assert!(matches!(err, Error::InvalidPath(_)));
    }

    #[test]
    fn paths_are_root_relative_with_optional_leading_slash() {
        let path = NfsPath::file("/a/b.txt").unwrap();
        assert_eq!(path.components(), &["a", "b.txt"]);

        let dir = NfsPath::directory("/a/b/").unwrap();
        assert_eq!(dir.components(), &["a", "b"]);

        let dir = NfsPath::directory("///a/b///").unwrap();
        assert_eq!(dir.components(), &["a", "b"]);

        let root = NfsPath::directory("/").unwrap();
        assert!(root.components().is_empty());
    }

    #[test]
    fn short_paths_store_components_inline_and_long_paths_spill_to_heap() {
        let short = NfsPath::file("/a/b/c/d").unwrap();
        assert_eq!(short.components(), &["a", "b", "c", "d"]);
        assert!(!short.components.spilled());

        let long = NfsPath::file("/a/b/c/d/e").unwrap();
        assert_eq!(long.components(), &["a", "b", "c", "d", "e"]);
        assert!(long.components.spilled());
    }

    #[test]
    fn spilled_paths_keep_headroom_for_more_components() {
        let path = NfsPath::file("/a/b/c/d/e/f").unwrap();

        assert_eq!(path.components(), &["a", "b", "c", "d", "e", "f"]);
        assert!(path.components.spilled());
        assert!(path.components.capacity() >= path.components.len());
    }

    #[test]
    fn export_paths_borrow_components_without_owning_strings() {
        let export = String::from("/exports/data");
        let path = parse_export(&export).unwrap();
        assert_eq!(path.components(), &["exports", "data"]);
        assert!(!path.components.spilled());
        assert_eq!(parse_export("/").unwrap().components(), &[] as &[&str]);
    }
}
