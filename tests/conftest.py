import zipfile
from io import BytesIO
from pathlib import Path

import lxml.html
import pytest
import requests
from tqdm import tqdm

import polars_io as pio
from tests import DATA

MANY_FILES_PER_PAGE = {
    "dta": [
        # stata included files
        "https://principlesofeconometrics.com/stata.htm",
        # oi credit
        "https://opportunityinsights.org/data/?geographic_level=0&topic=0&paper_id=5359#resource-listing",
    ],
    "sas7bdat": [
        # "https://www.alanelliott.com/sased2/ED2_FILES.html", # FIX: this fails on github actions
        "https://www.principlesofeconometrics.com/sas.htm",
    ],
    "xpt": [
        "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Laboratory&Cycle=2021-2023",
    ],
}


SINGLE_COMPRESSED_FILE = {
    "sas7bdat": [
        "https://gss.norc.org/Documents/sas/GSS_sas.zip",
        "https://libguides.library.kent.edu/ld.php?content_id=11205331",
    ],
    "dta": [
        "https://gss.norc.org/documents/stata/GSS_stata.zip",
    ],
    "xpt": [
        "https://www.cdc.gov/brfss/annual_data/2023/files/LLCP2023XPT.zip",
    ],
}


def pytest_generate_tests(metafunc: pytest.Metafunc):
    """Main test-generation function"""
    test_name = metafunc.function.__name__
    suffix = test_name.split("_")[-1]

    if "eager" in test_name and suffix in MANY_FILES_PER_PAGE:
        generate_test_cases(metafunc, high_bytes=10e6)

    if "lazy" in test_name and suffix in SINGLE_COMPRESSED_FILE:
        generate_test_cases(metafunc, low_bytes=10e6, high_bytes=10e9)


def generate_test_cases(
    metafunc: pytest.Metafunc,
    *,
    low_bytes=-float("inf"),
    high_bytes=float("inf"),
):
    suffix = metafunc.function.__name__.split("_")[-1]

    path = get_data_for_filetype(suffix)

    files = [
        file
        for file in path.glob(f"*.{suffix}")
        if file.stat().st_size > low_bytes and file.stat().st_size < high_bytes
    ]

    metafunc.parametrize("file", files)


def decompress_if_needed(url: str, content: bytes, suffix: str) -> tuple[str, bytes]:
    if not url.endswith(".zip"):
        return (url.rsplit("/", 1)[-1] + "." + suffix, content)

    with zipfile.ZipFile(BytesIO(content)) as zf:
        # get first file that we can read
        print([f.filename.lower().strip() for f in zf.filelist])
        name = next(
            f.filename
            for f in zf.filelist
            if pio._get_scanning_function(f.filename) is not None
        )
        return Path(name).parts[-1], zf.read(name)


def download_and_decompress_single_file(*, url: str, save_to: Path, suffix: str):
    print(f"Downloading {url}")

    with requests.get(url) as r:
        name, file = decompress_if_needed(url, r.content, suffix)
        print(f"Saving {name} to {save_to}")

    save_to.mkdir(exist_ok=True, parents=True)
    (save_to / name.lower().strip()).write_bytes(file)


def download_every_linked_file_with_suffix(*, url: str, save_to: Path, suffix: str):
    with requests.get(url) as r:
        tree = lxml.html.fromstring(r.text, base_url=url)

    tree.make_links_absolute()

    files_to_download = [
        link for link in tree.xpath("//a/@href") if link.endswith(suffix)
    ]

    save_to.mkdir(parents=True, exist_ok=True)

    for f in tqdm(files_to_download, desc=f"Getting {suffix} test files"):
        with requests.get(f) as r:
            (save_to / f.rsplit("/", 1)[-1]).write_bytes(r.content)


def get_data_for_filetype(suffix: str):
    path = DATA / suffix

    if not path.exists():
        for url in MANY_FILES_PER_PAGE[suffix]:
            print(f"Getting {suffix} files from {url}")
            download_every_linked_file_with_suffix(url=url, save_to=path, suffix=suffix)

        for url in SINGLE_COMPRESSED_FILE[suffix]:
            download_and_decompress_single_file(url=url, save_to=path, suffix=suffix)

    return path
