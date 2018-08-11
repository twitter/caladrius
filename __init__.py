import warnings

# Prevent warnings about benign issues with numpy.dtype default size changes
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

