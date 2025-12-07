import logging
import sys

_logging_configured = False

def setup_logging():
    global _logging_configured
    
    # Use both flag and handler check for robustness
    if _logging_configured:
        # Double-check: if flag is set but handlers were cleared, reconfigure
        root = logging.getLogger()
        has_stdout_handler = any(
            isinstance(h, logging.StreamHandler) and h.stream == sys.stdout 
            for h in root.handlers
        )
        if has_stdout_handler:
            return
    
    root = logging.getLogger()
    
    # Remove ALL existing handlers to ensure clean state
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    
    # Check if we already have a stdout handler with our formatter
    fmt = "[%(asctime)s] [%(levelname)s] %(message)s"
    formatter = logging.Formatter(fmt)
    
    existing_handler = None
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout:
            if h.formatter and h.formatter._fmt == fmt:
                existing_handler = h
                break
    
    if not existing_handler:
        # Configure new handler only if we don't have one already
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root.addHandler(handler)
    else:
        # Use existing handler, just ensure formatter is correct
        if not existing_handler.formatter or existing_handler.formatter._fmt != fmt:
            existing_handler.setFormatter(formatter)
    
    root.setLevel(logging.INFO)
    
    # Final check: ensure only one stdout handler exists
    stdout_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout]
    if len(stdout_handlers) > 1:
        # Keep only the first one, remove the rest
        for h in stdout_handlers[1:]:
            root.removeHandler(h)
            try:
                h.close()
            except:
                pass
    
    _logging_configured = True
