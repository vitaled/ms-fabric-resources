#!/usr/bin/env python3
"""
Setup script to download the AMO/TOM .NET libraries required by aas_to_fabric.py.

This downloads the Microsoft.AnalysisServices.retail.amd64 NuGet package
(.NET Framework version) and extracts the relevant DLLs into a local
dotnet_libs/ folder.

Usage:
    python setup_dotnet.py
"""

import io
import os
import sys
import shutil
import zipfile
import logging
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is required.  Run:  pip install requests")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("setup_dotnet")

# NuGet package details — using the .NET Framework (net45) version which works
# with pythonnet's netfx runtime on Windows.
PACKAGES = [
    {
        "name": "Microsoft.AnalysisServices.retail.amd64",
        "version": "19.84.1",
        "url": "https://www.nuget.org/api/v2/package/Microsoft.AnalysisServices.retail.amd64/19.84.1",
        "dlls": [
            "lib/net45/Microsoft.AnalysisServices.Core.dll",
            "lib/net45/Microsoft.AnalysisServices.Tabular.dll",
            "lib/net45/Microsoft.AnalysisServices.Tabular.Json.dll",
            "lib/net45/Microsoft.AnalysisServices.dll",
        ],
    },
]

TARGET_DIR = Path(__file__).parent / "dotnet_libs"


def download_and_extract():
    """Download NuGet packages and extract the required DLLs."""
    TARGET_DIR.mkdir(exist_ok=True)

    for pkg in PACKAGES:
        log.info("Downloading %s v%s ...", pkg["name"], pkg["version"])

        resp = requests.get(pkg["url"], allow_redirects=True, stream=True)
        resp.raise_for_status()

        # NuGet packages are ZIP files
        content = io.BytesIO(resp.content)

        with zipfile.ZipFile(content) as zf:
            all_files = zf.namelist()

            for dll_path in pkg["dlls"]:
                # NuGet paths may use forward slashes
                matches = [f for f in all_files if f.replace("\\", "/").endswith(dll_path.replace("\\", "/"))]
                if not matches:
                    # Try a case-insensitive or partial match
                    dll_name = os.path.basename(dll_path)
                    matches = [f for f in all_files if f.endswith(dll_name)]

                if matches:
                    src = matches[0]
                    dll_name = os.path.basename(src)
                    dest = TARGET_DIR / dll_name

                    with zf.open(src) as src_f, open(dest, "wb") as dst_f:
                        shutil.copyfileobj(src_f, dst_f)

                    log.info("  Extracted: %s → %s", src, dest)
                else:
                    log.warning("  DLL not found in package: %s", dll_path)
                    log.debug("  Available files: %s", [f for f in all_files if f.endswith(".dll")])

    log.info("Done. DLLs are in: %s", TARGET_DIR.resolve())
    log.info(
        "\nVerify the setup by running:\n"
        "  python -c \"import clr; clr.AddReference(r'%s\\Microsoft.AnalysisServices.Tabular'); print('OK')\"",
        TARGET_DIR.resolve(),
    )


if __name__ == "__main__":
    download_and_extract()
