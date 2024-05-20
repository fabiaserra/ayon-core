import os

from ayon_core.lib import (
    Logger,
    get_oiio_info_for_input,
    path_tools
)


IN_COLORSPACES = [
    "sRGB",
    "Linear",
    "Raw",
]

BIT_DEPTHS_SRGB = [
    "uint8",
    "int8",
    "int16"
]

LINEAR_EXTS = [
    ".exr"
]

RAW_FUZZY_NAMES = {
    "disp",
    "displacement",
    "normal"
}

logger = Logger.get_logger(__name__)


def guess_colorspace(filepath):
    """Guess the colorspace of the input image filename.

    Returns:
        str: a string suitable for the --colorconvert option of maketx (linear, sRGB, Rec709)
    """
    source_files, ext, _ = path_tools.convert_to_sequence(
        filepath
    )
    if not source_files:
        return
    
    if ext in LINEAR_EXTS:
        return "linear"
    
    single_file = source_files[0]
    try:
        img_info = get_oiio_info_for_input(single_file)
    except RuntimeError:
        return None

    in_colorspace = img_info.get("oiio:ColorSpace")
    if in_colorspace:
        return in_colorspace
    elif img_info["format"] in BIT_DEPTHS_SRGB:
        return "sRGB"
    elif any([name in single_file.lower() for name in RAW_FUZZY_NAMES]):
        return "Raw"

    return "linear"
