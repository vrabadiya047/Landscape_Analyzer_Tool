import sys
from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine-tuning.
build_exe_options = {
    "packages": ["os", "sys", "PyQt5"],
    "include_files": ["logo.png"],  # Add your splash screen image here
    "excludes": []
}

# Base "Win32GUI" should be used to suppress the console for GUI applications.
base = None
if sys.platform == "win32":
    base = "Win32GUI"

setup(
    name="LandscapeAnalyzer",
    version="1.0",
    description="My PyQt5 application with a splash screen",
    options={"build_exe": build_exe_options},
    executables=[Executable("test.py", base=base, icon="logo.ico")]
)
