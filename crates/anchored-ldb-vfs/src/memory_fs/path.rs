#![expect(
    unsafe_code,
    reason = "transmuting Path into NormalizedPath without `ref-cast` and `syn`, \
              and preserving the invariants of `NormalizedPath` and `NormalizedPathBuf`",
)]
#![expect(
    clippy::missing_const_for_fn,
    reason = "`Path` and `PathBuf` cannot be constructed with `const` functions yet, anyway",
)]
// Most items in this file should be marked `#[inline]`.
#![warn(clippy::missing_inline_in_public_items)]

use std::{borrow::Borrow, ops::Deref};
use std::path::{Component, Path, PathBuf};


/// A normalized [`PathBuf`] which is an absolute path containing no `..`, `.`, or [Windows prefix]
/// components. Unsafe code may rely on this invariant.
///
/// See also [`NormalizedPath`] for the slice version.
///
/// [Windows prefix]: Component::Prefix
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NormalizedPathBuf(PathBuf);

impl NormalizedPathBuf {
    /// Check whether a path is absolute (starts with `/`) and contains no `..`, `.`, or
    /// [Windows prefix] components.
    ///
    /// [Windows prefix]: Component::Prefix
    #[expect(
        clippy::missing_inline_in_public_items,
        reason = "this function is of nontrivial length, unlike most functions in this file",
    )]
    #[must_use]
    pub fn is_normalized(path: &Path) -> bool {
        let mut components = path.components();

        // A `RootDir` component must be at the start of the path.
        if components.next() != Some(Component::RootDir) {
            return false;
        }

        // The rest of the path should be normal components (no `..` or `.`).
        components.all(|component| matches!(component, Component::Normal(_)))
    }

    /// Create a new (normalized) path to the root directory.
    #[inline]
    #[must_use]
    pub fn root() -> Self {
        Self(Path::new("/").to_owned())
    }

    /// Normalize a [`Path`] by evaluating `..` and `.` components, and converting any path into
    /// an absolute path, starting with `/`. Any [Windows prefix] is ignored, and relative paths
    /// are treated as relative paths starting at the root directory (i.e., as equivalent to
    /// absolute paths).
    ///
    /// Note that in the root directory, using `..` is not an error, and refers to the root
    /// directory.
    ///
    /// For existing work, see [`NormalizePath::normalize`] from the [normalize-path] crate,
    /// especially that method's [implementation for `Path`]. The function here provides what is
    /// needed for a `MemoryFS`, but an actual filesystem will probably care about relative paths
    /// and, on Windows, prefixes.
    ///
    /// [Windows prefix]: Component::Prefix
    /// [`NormalizePath::normalize`]: https://docs.rs/normalize-path/0.2.1/normalize_path/trait.NormalizePath.html#tymethod.normalize
    /// [normalize-path]: https://crates.io/crates/normalize-path/0.2.1
    /// [implementation for `Path`]: https://docs.rs/normalize-path/0.2.1/src/normalize_path/lib.rs.html#47-74
    #[expect(
        clippy::missing_inline_in_public_items,
        reason = "this function is of nontrivial length, unlike most functions in this file",
    )]
    #[must_use]
    pub fn new(path: &Path) -> Self {
        let mut normalized = Path::new("/").to_owned();

        for component in path.components() {
            // We ignore any Windows prefix like `C:`, ignore the root directory since it can
            // only possibly appear at the start of the path and we always add one above,
            // and ignore the `.` component since it does nothing.
            match component {
                Component::Prefix(_) | Component::RootDir | Component::CurDir => {},
                Component::ParentDir => {
                    // Note that `pop` does nothing if `normalized.parent()` is `None`,
                    // and that's the case for the root directory. Therefore, the "/" that we add
                    // when we first construct `normalized` can never be popped away.
                    normalized.pop();
                }
                Component::Normal(component) => {
                    normalized.push(component);
                }
            }
        }

        Self(normalized)
    }

    /// Attempt to create a `NormalizedPathBuf` from a [`PathBuf`], returning `None` if the path
    /// isn't already [normalized].
    ///
    /// [normalized]: NormalizedPathBuf::is_normalized
    #[inline]
    #[must_use]
    pub fn new_checked(path: PathBuf) -> Option<Self> {
        if Self::is_normalized(&path) {
            // SAFETY:
            // We've just confirmed that `NormalizedPathBuf::is_normalized(&path)` is true.
            Some(unsafe { Self::new_unchecked(path) })
        } else {
            None
        }
    }

    /// Create a `NormalizedPathBuf` from a [`PathBuf`] without checking whether the path is
    /// actually [normalized].
    ///
    /// # Safety
    /// The provided `path` must be normalized; that is, the path must be an absolute path without
    /// any `.`, `..`, or Windows prefix components. Equivalently,
    /// `NormalizedPathBuf::is_normalized(&path)` must return true.
    ///
    /// [normalized]: NormalizedPathBuf::is_normalized
    #[inline]
    #[must_use]
    pub unsafe fn new_unchecked(path: PathBuf) -> Self {
        Self(path)
    }

    /// Consume the normalized path and return its inner [`PathBuf`].
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> PathBuf {
        self.0
    }

    /// Get a reference to the inner [`PathBuf`].
    #[inline]
    #[must_use]
    pub fn as_path_buf(&self) -> &PathBuf {
        &self.0
    }

    /// Coerces to a [`Path`] slice.
    #[inline]
    #[must_use]
    pub fn as_path(&self) -> &Path {
        self
    }

    /// Coerces to a [`NormalizedPath`] slice.
    #[inline]
    #[must_use]
    pub fn as_normalized_path(&self) -> &NormalizedPath {
        self
    }
}

impl NormalizedPathBuf {
    /// Return a path which, relative to `new`, is in the same position that `self` is
    /// relative to `old`.
    ///
    /// # Panics
    /// Panics if `self` is neither equal to `old` nor a recursive child of `old`.
    /// That is, `self.starts_with(old)` must be true.
    pub(super) fn move_to_new_branch(
        self,
        old:      &NormalizedPath,
        new:      &NormalizedPath,
    ) -> Self {
        #[expect(
            clippy::unwrap_used,
            reason = "The condition asserted by the caller implies that `strip_prefix` is `Some`",
        )]
        let rel_path = self.strip_prefix(old).unwrap();

        // Note that both `self` and `old` are `NormalizedPath`s. By the invariants
        // of `NormalizedPath`, we thus know that both `self` and `old` are absolute
        // and neither have prefixes. Therefore, `old` at least contains `/`,
        // so at least the root directory is stripped from `self`, and thus
        // the resulting `rel_path` value is genuinely a relative path.
        // Therefore, joining `new` and `rel_path` does not completely ignore the contents
        // of new; `new` becomes a prefix of `new_path`. This ensures semantic correctness.
        let new_path = new.join(rel_path);

        // SAFETY:
        // `new` is an absolute path, so therefore `new_path` is as well.
        // Additionally, neither `self` nor `new` contain any `.`, `..`, or Windows
        // prefix component (by the invariants of `NormalizedPath{Buf,}`).
        // Therefore, as `rel_path` is a substring of `self`, and every component in
        // `new_path` is in `new` or `rel_path` (or both), it follows that `new_path`
        // does not contain those components either. Thus, `new_path` is normalized.
        unsafe { Self::new_unchecked(new_path) }
    }
}

impl Deref for NormalizedPathBuf {
    type Target = NormalizedPath;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY:
        // The path inside `self` is normalized, by the invariant of `NormalizedPathBuf`,
        // which is essentially the same as the invariant of `NormalizedPath`.
        unsafe { NormalizedPath::new_unchecked(&self.0) }
    }
}

impl AsRef<NormalizedPath> for NormalizedPathBuf {
    #[inline]
    fn as_ref(&self) -> &NormalizedPath {
        self
    }
}

impl AsRef<PathBuf> for NormalizedPathBuf {
    #[inline]
    fn as_ref(&self) -> &PathBuf {
        &self.0
    }
}

impl AsRef<Path> for NormalizedPathBuf {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
    }
}

impl Borrow<NormalizedPath> for NormalizedPathBuf {
    #[inline]
    fn borrow(&self) -> &NormalizedPath {
        self
    }
}

impl Borrow<PathBuf> for NormalizedPathBuf {
    #[inline]
    fn borrow(&self) -> &PathBuf {
        &self.0
    }
}

impl Borrow<Path> for NormalizedPathBuf {
    #[inline]
    fn borrow(&self) -> &Path {
        self
    }
}

impl PartialEq<NormalizedPath> for NormalizedPathBuf {
    #[inline]
    fn eq(&self, other: &NormalizedPath) -> bool {
        let lhs: &NormalizedPath = self;
        lhs == other
    }
}

impl PartialEq<NormalizedPathBuf> for NormalizedPath {
    #[inline]
    fn eq(&self, other: &NormalizedPathBuf) -> bool {
        let rhs: &Self = other;
        self == rhs
    }
}

/// A normalized [`Path`] which is an absolute path containing no `..`, `.`, or Windows
/// prefix components. Unsafe code may rely on this invariant.
///
/// See also [`NormalizedPathBuf`] for an owned version. Creating a normalized path from a path
/// which is not normalized may require a new allocation, so newly-normalized paths are
/// constructed via [`NormalizedPathBuf`].
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NormalizedPath(Path);

impl NormalizedPath {
    /// Check whether a path is absolute (starts with `/`) and contains no `..`, `.`, or
    /// [Windows prefix] components.
    ///
    /// Convenience method which delegates to [`NormalizedPathBuf::is_normalized`].
    ///
    /// [Windows prefix]: Component::Prefix
    #[inline]
    #[must_use]
    pub fn is_normalized(path: &Path) -> bool {
        NormalizedPathBuf::is_normalized(path)
    }

    /// Create a new (normalized) path to the root directory.
    #[inline]
    #[must_use]
    pub fn root() -> &'static Self {
        // SAFETY:
        // The path `/` starts with a `/` and contains no `..`, `.`, or Windows prefix
        // components. It's the root directory. It's normalized.
        unsafe { Self::new_unchecked(Path::new("/")) }
    }

    /// Attempt to create a `NormalizedPath` from a [`Path`], returning `None` if the path
    /// isn't already [normalized].
    ///
    /// [normalized]: NormalizedPathBuf::is_normalized
    #[inline]
    #[must_use]
    pub fn new_checked(path: &Path) -> Option<&Self> {
        if Self::is_normalized(path) {
            // SAFETY:
            // We've just confirmed that `NormalizedPath::is_normalized(path)` is true.
            Some(unsafe { Self::new_unchecked(path) })
        } else {
            None
        }
    }

    /// Create a `NormalizedPath` from a [`Path`] without checking whether the path is
    /// actually [normalized].
    ///
    /// # Safety
    /// The provided `path` must be normalized; that is, the path must be an absolute path without
    /// any `.`, `..`, or Windows prefix components. Equivalently,
    /// `NormalizedPath::is_normalized(path)` must return true.
    ///
    /// [normalized]: NormalizedPath::is_normalized
    #[inline]
    #[must_use]
    pub unsafe fn new_unchecked(path: &Path) -> &Self {
        // Showing the types involved explicitly.
        let path: *const Path = path;
        #[expect(clippy::as_conversions, reason = "no other option AFAIK")]
        let path: *const Self = path as *const Self;

        // SAFETY:
        // The `path` pointer came from a valid reference to a `Path` type, and since
        // `NormalizedPath` is `#[repr(transparent)]` without any `#[repr(packed)]` or
        // `#[repr(aligned)]` setting, it defers all layout requirements to `Path`.
        // Note that `Path` itself is a wrapper around `OsStr`, and `Path::new` does
        // essentially the same code as this. Transmuting an inner type into a transparent
        // wrapper is a common task known to be safe, but here's the checklist anyway:
        //   - The pointer is properly aligned, since `Path` and `NormalizedPath` have
        //     the same alignment.
        //   - The pointer is non-null, since every valid reference (like the one we were given)
        //     is non-null.
        //   - It is dereferenceable, as the fat pointer comes unmodified from a `Path` allocation,
        //     so the memory accessed by the below dereference lies entirely within a single
        //     allocation.
        //   - The pointer points to a valid value of type `Path`, which is *also* a valid
        //     value of type `NormalizedPath` as guaranteed by `#[repr(transparent)]`.
        //   - Aliasing rules are enforced by the signature of this function.
        //     We are given an immutable reference of lifetime '_, and return an immutable
        //     reference of lifetime '_.
        unsafe { &*path }
    }

    /// Return the parent of `self`, unless `self` is the root directory, in which case `None` is
    /// returned.
    #[inline]
    #[must_use]
    pub fn normalized_parent(&self) -> Option<&Self> {
        self.0
            .parent()
            .map(|parent| {
                // SAFETY:
                // `self.0.parent()` returns a path containing all but the final component of the
                // path, unless the path is empty or ends with a root or prefix.
                // Therefore, `self.0.parent()` never strips away the initial `/` for the root
                // directory. Moreover, it certainly never adds in a `.`, `..`, or Windows
                // prefix component. Therefore, the parent path is normalized (if it exists,
                // as it does in this branch).
                unsafe { Self::new_unchecked(parent) }
            })
    }
}

impl ToOwned for NormalizedPath {
    type Owned = NormalizedPathBuf;

    #[inline]
    fn to_owned(&self) -> Self::Owned {
        NormalizedPathBuf(self.0.to_owned())
    }

    #[inline]
    fn clone_into(&self, target: &mut Self::Owned) {
        let path_buf = &mut target.0;
        self.0.clone_into(path_buf);
    }
}

impl Deref for NormalizedPath {
    type Target = Path;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for NormalizedPath {
    #[inline]
    fn as_ref(&self) -> &Path {
        self
    }
}

impl Borrow<Path> for NormalizedPath {
    #[inline]
    fn borrow(&self) -> &Path {
        self
    }
}
