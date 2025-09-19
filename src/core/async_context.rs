use crate::core::status::Status;

/// The `IAsyncContext` trait defines an interface for asynchronous operation contexts.
/// Corresponds to `IAsyncContext` class in C++ `core/async.h`.
///
/// Implementors of this trait must be `Send` and `Sync` because they can be moved
/// between threads for asynchronous operations. The `'static` lifetime bound ensures
/// that the context does not contain any borrowed references.
pub trait IAsyncContext: Send + Sync + 'static {
    /// Creates a new heap-allocated copy of the context.
    /// This method corresponds to C++'s `DeepCopy_Internal`.
    ///
    /// Implementors should return a `Box<dyn IAsyncContext>` containing a clone of `self`.
    fn clone_box(&self) -> Box<dyn IAsyncContext>;
}

/// Type alias for an asynchronous I/O callback function.
/// Corresponds to `AsyncIOCallback` in C++ `core/async.h`.
///
/// The `context` is a `Box<dyn IAsyncContext>` indicating ownership transfer.
pub type AsyncIoCallbackFn =
    fn(context: Box<dyn IAsyncContext>, result: Status, bytes_transferred: usize);

/// Type alias for a general asynchronous callback function.
/// Corresponds to `AsyncCallback` in C++ `core/async.h`.
///
/// The `context` is a `Box<dyn IAsyncContext>` indicating ownership transfer.
pub type AsyncCallbackFn = fn(context: Box<dyn IAsyncContext>, result: Status);

/// Macro to propagate `Status::IoError` or `Status::OutOfMemory` errors.
/// Corresponds to `RETURN_NOT_OK(s)` macro in C++ `core/async.h`.
///
/// This macro is designed to be used within functions that return `Result<T, Status>`.
/// If the provided `status_expr` is not `Status::Ok`, it will return early with that status.
#[macro_export]
macro_rules! return_not_ok {
    ($status_expr:expr) => {
        let status = $status_expr;
        if status != $crate::core::status::Status::Ok {
            return Err(status);
        }
    };
}
