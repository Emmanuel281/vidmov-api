import logging
import time
import functools
from typing import Dict, Any, Callable

class Logger:
    """
    Helper class untuk logging yang terstruktur dan mudah di-trace.
    
    Usage:
        logger = Logger(__name__)
        logger.info("User login", user_id="123", action="login")
        logger.error("Database error", error=str(e), query="SELECT * FROM users")
    """
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
    
    def _log(self, level: int, message: str, **kwargs):
        """Internal method untuk format log message"""
        if kwargs:
            # Format extra data sebagai key=value pairs
            extra_data = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            full_message = f"{message} | {extra_data}"
        else:
            full_message = message
        
        # stacklevel=3 means: skip this method and get caller info
        # This will show the actual caller (create(), update(), etc) instead of _log()
        self.logger.log(level, full_message, stacklevel=3)
    
    def debug(self, message: str, **kwargs):
        """Log level DEBUG - untuk development/debugging"""
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log level INFO - untuk informasi normal"""
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log level WARNING - untuk peringatan"""
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log level ERROR - untuk error yang bisa di-recover"""
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log level CRITICAL - untuk error fatal"""
        self._log(logging.CRITICAL, message, **kwargs)
    
    def exception(self, message: str, exc_info=True, **kwargs):
        """Log exception dengan stack trace"""
        if kwargs:
            extra_data = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            full_message = f"{message} | {extra_data}"
        else:
            full_message = message
        
        # stacklevel=2 to get caller info, not this method
        self.logger.exception(full_message, exc_info=exc_info, stacklevel=3)
    
    # Shorthand methods untuk use case umum
    def log_operation(self, operation: str, status: str, **kwargs):
        """
        Log operasi dengan status.
        
        Example:
            logger.log_operation("create_user", "success", user_id="123")
            logger.log_operation("update_data", "failed", error="Not found")
        """
        self.info(f"Operation: {operation}", status=status, **kwargs)
    
    def log_db_operation(self, operation: str, collection: str, status: str, **kwargs):
        """
        Log operasi database.
        
        Example:
            logger.log_db_operation("insert", "_user", "success", doc_id="123")
        """
        self.info(
            f"DB Operation: {operation}",
            collection=collection,
            status=status,
            **kwargs
        )
    
    def log_api_call(self, method: str, endpoint: str, status_code: int, duration_ms: float, **kwargs):
        """
        Log API call.
        
        Example:
            logger.log_api_call("POST", "/v1/users", 200, 150.5, user_id="123")
        """
        self.info(
            f"API Call: {method} {endpoint}",
            status_code=status_code,
            duration_ms=duration_ms,
            **kwargs
        )
    
    def log_error_with_context(self, error: Exception, context: Dict[str, Any]):
        """
        Log error dengan context lengkap.
        
        Example:
            try:
                # some code
            except Exception as e:
                logger.log_error_with_context(e, {
                    "user_id": user_id,
                    "action": "update",
                    "data": data
                })
        """
        self.exception(
            f"Error: {type(error).__name__} - {str(error)}",
            **context
        )

def log_execution_time(logger: Logger = None):
    """
    Decorator untuk log execution time fungsi.
    
    Usage:
        @log_execution_time()
        def my_function():
            # code here
    """
    def decorator(func: Callable) -> Callable:
        # Buat logger jika tidak disediakan
        nonlocal logger
        if logger is None:
            logger = Logger(f"{func.__module__}.{func.__name__}")
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            func_name = func.__name__
            
            logger.debug(f"Function started: {func_name}")
            
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                
                logger.info(
                    f"Function completed: {func_name}",
                    duration_ms=round(duration_ms, 2),
                    status="success"
                )
                
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"Function failed: {func_name}",
                    duration_ms=round(duration_ms, 2),
                    error=str(e),
                    error_type=type(e).__name__
                )
                raise
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            func_name = func.__name__
            
            logger.debug(f"Async function started: {func_name}")
            
            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                
                logger.info(
                    f"Async function completed: {func_name}",
                    duration_ms=round(duration_ms, 2),
                    status="success"
                )
                
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                logger.error(
                    f"Async function failed: {func_name}",
                    duration_ms=round(duration_ms, 2),
                    error=str(e),
                    error_type=type(e).__name__
                )
                raise
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def log_function_call(logger: Logger = None, log_args: bool = False):
    """
    Decorator untuk log function calls dengan arguments (opsional).
    
    Usage:
        @log_function_call(log_args=True)
        def create_user(username: str, email: str):
            # code here
    """
    def decorator(func: Callable) -> Callable:
        nonlocal logger
        if logger is None:
            logger = Logger(f"{func.__module__}.{func.__name__}")
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            func_name = func.__name__
            
            log_data = {"function": func_name}
            
            if log_args:
                # Log positional args (be careful with sensitive data!)
                if args:
                    log_data["args"] = str(args)[:200]  # Limit length
                # Log keyword args
                if kwargs:
                    # Filter out sensitive keys
                    safe_kwargs = {
                        k: v for k, v in kwargs.items() 
                        if k not in ['password', 'token', 'secret', 'api_key']
                    }
                    log_data["kwargs"] = str(safe_kwargs)[:200]
            
            logger.debug(f"Calling function: {func_name}", **log_data)
            
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Function returned: {func_name}")
                return result
            except Exception as e:
                logger.exception(f"Function raised exception: {func_name}", error=str(e))
                raise
        
        return wrapper
    
    return decorator


class LogTimer:
    """
    Context manager untuk mengukur execution time block code.
    
    Usage:
        with LogTimer(logger, "Database query"):
            result = collection.find_one(query)
    """
    def __init__(self, logger: Logger, operation: str, **context):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.debug(f"Starting: {self.operation}", **self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        
        if exc_type is None:
            # Success
            self.logger.info(
                f"Completed: {self.operation}",
                duration_ms=round(duration_ms, 2),
                status="success",
                **self.context
            )
        else:
            # Error
            self.logger.error(
                f"Failed: {self.operation}",
                duration_ms=round(duration_ms, 2),
                status="failed",
                error=str(exc_val),
                error_type=exc_type.__name__,
                **self.context
            )
        
        # Don't suppress exception
        return False


class PerformanceLogger:
    """
    Helper untuk log performance metrics dengan thresholds.
    
    Usage:
        perf = PerformanceLogger(logger, slow_threshold_ms=1000)
        perf.log_operation("db_query", 1500)  # Will log as WARNING
    """
    def __init__(self, logger: Logger, slow_threshold_ms: float = 1000):
        self.logger = logger
        self.slow_threshold_ms = slow_threshold_ms
    
    def log_operation(self, operation: str, duration_ms: float, **context):
        """Log operation dengan automatic level based on duration"""
        if duration_ms > self.slow_threshold_ms:
            self.logger.warning(
                f"Slow operation: {operation}",
                duration_ms=round(duration_ms, 2),
                threshold_ms=self.slow_threshold_ms,
                **context
            )
        else:
            self.logger.info(
                f"Operation: {operation}",
                duration_ms=round(duration_ms, 2),
                **context
            )


def sanitize_log_data(data: dict, sensitive_keys: list = None) -> dict:
    """
    Remove sensitive data from dict before logging.
    
    Usage:
        safe_data = sanitize_log_data(user_data, ['password', 'ssn'])
        logger.info("User data", **safe_data)
    """
    if sensitive_keys is None:
        sensitive_keys = [
            'password', 'token', 'secret', 'api_key', 
            'credit_card', 'ssn', 'private_key'
        ]
    
    sanitized = {}
    for key, value in data.items():
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            sanitized[key] = "***REDACTED***"
        elif isinstance(value, dict):
            sanitized[key] = sanitize_log_data(value, sensitive_keys)
        else:
            sanitized[key] = value
    
    return sanitized