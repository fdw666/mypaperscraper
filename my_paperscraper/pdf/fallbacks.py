"""Functionalities to scrape PDF files of publications."""

import calendar
import datetime
import io
import logging
import re
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Union

import boto3
import requests
from lxml import etree
from tqdm import tqdm
from bs4 import BeautifulSoup

ELIFE_XML_INDEX = None  # global variable to cache the eLife XML index from GitHub

logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def fallback_wiley_api(
    paper_metadata: Dict[str, Any],
    output_path: Path,
    api_keys: Dict[str, str],
    max_attempts: int = 2,
):
    """
    Attempt to download the PDF via the Wiley TDM API (popular publisher which blocks standard scraping attempts; API access free for academic users).

    This function uses the WILEY_TDM_API_TOKEN environment variable to authenticate
    with the Wiley TDM API and attempts to download the PDF for the given paper.
    See https://onlinelibrary.wiley.com/library-info/resources/text-and-datamining for a description on how to get your WILEY_TDM_API_TOKEN.

    Args:
        paper_metadata (dict): Dictionary containing paper metadata. Must include the 'doi' key.
        output_path (Path): A pathlib.Path object representing the path where the PDF will be saved.
        api_keys (dict): Preloaded API keys.
        max_attempts (int): The maximum number of attempts to retry API call.

    Returns:
        bool: True if the PDF file was successfully downloaded, False otherwise.
    """

    WILEY_TDM_API_TOKEN = api_keys.get("WILEY_TDM_API_TOKEN")
    encoded_doi = paper_metadata["doi"].replace("/", "%2F")
    api_url = f"https://api.wiley.com/onlinelibrary/tdm/v1/articles/{encoded_doi}"
    headers = {"Wiley-TDM-Client-Token": WILEY_TDM_API_TOKEN}

    attempt = 0
    success = False

    while attempt < max_attempts:
        try:
            api_response = requests.get(
                api_url, headers=headers, allow_redirects=True, timeout=60
            )
            api_response.raise_for_status()
            if api_response.content[:4] != b"%PDF":
                logger.warning(
                    f"API returned content that is not a valid PDF for {paper_metadata['doi']}."
                )
            else:
                with open(output_path.with_suffix(".pdf"), "wb+") as f:
                    f.write(api_response.content)
                logger.info(
                    f"Successfully downloaded PDF via Wiley API for {paper_metadata['doi']}."
                )
                success = True
                break
        except Exception as e2:
            if attempt < max_attempts - 1:
                logger.info("Waiting 20 seconds before retrying...")
                time.sleep(20)
            logger.error(
                f"Could not download via Wiley API (attempt {attempt + 1}/{max_attempts}): {e2}"
            )

        attempt += 1

    # **Mandatory delay of 10 seconds to comply with Wiley API rate limits**
    logger.info(
        "Waiting 10 seconds before next request to comply with Wiley API rate limits..."
    )
    time.sleep(10)
    return success


def fallback_bioc_pmc(doi: str, output_path: Path,proxies=None):
    """
    Attempt to download the XML via the BioC-PMC fallback.

    This function first converts a given DOI to a PMCID using the NCBI ID Converter API.
    If a PMCID is found, it constructs the corresponding PMC XML URL and attempts to
    download the full-text XML.

    PubMed Central® (PMC) is a free full-text archive of biomedical and life sciences
    journal literature at the U.S. National Institutes of Health's National Library of Medicine (NIH/NLM).

    Args:
        doi (str): The DOI of the paper to retrieve.
        output_path (Path): A pathlib.Path object representing the path where the XML file will be saved.

    Returns:
        bool: True if the XML file was successfully downloaded, False otherwise.
    """
    ncbi_tool = "paperscraper"
    ncbi_email = "2987498538@qq.com"

    converter_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
    params = {
        "tool": ncbi_tool,
        "email": ncbi_email,
        "ids": doi,
        "idtype": "doi",
        "format": "json",
    }
    try:
        conv_response = requests.get(converter_url, params=params,proxies=proxies, timeout=20)
        conv_response.raise_for_status()
        data = conv_response.json()
        records = data.get("records", [])
        if not records or "pmcid" not in records[0]:
            # logger.warning(
            #     f"No PMCID available for DOI {doi}. Fallback via PMC therefore not possible."
            # )
            return False,None
        pmcid = records[0]["pmcid"]
        # logger.info(f"Converted DOI {doi} to PMCID {pmcid}.")
    except Exception as conv_err:
        # logger.error(f"Error during DOI to PMCID conversion: {conv_err}")
        return False,None
    #首先下载pdf
    # url = 'https://www.ncbi.nlm.nih.gov/pmc/articles/' + str(pmcid) + "/"
    # try:
    #     headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36",
    #                "Accept": "application/pdf,*/*;q=0.9"}
    #     request = requests.get(url, headers=headers,proxies=proxies,timeout=20)
    #     with open("test.html", "wb") as f:
    #         f.write(request.content)
    #     bs = BeautifulSoup(request.content, 'lxml')
    #     download_url = bs.find(attrs={'name':'citation_pdf_url'}).get('content')
    #     # print(download_url)
    #     r = requests.get(download_url, headers=headers,proxies=proxies,timeout=20)
    #     if r.content[:4] != b"%PDF":
    #         # print(r.content)
    #         return False
    #     with open(output_path.with_suffix(".pdf"), 'wb') as f:
    #         f.write(r.content)
    #     return True
    # except Exception:
    #     pass

    # Construct PMC XML URL
    xml_url = f"https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_xml/{pmcid}/unicode"
    # logger.info(f"Attempting to download XML from BioC-PMC URL: {xml_url}")
    try:
        xml_response = requests.get(xml_url, proxies=proxies,timeout=20)
        xml_response.raise_for_status()
        xml_path = output_path.with_suffix(".xml")
        # check for xml error:
        if xml_response.content.startswith(
            b"[Error] : No result can be found. <BR><HR><B> - https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/"
        ):
            # logger.warning(f"No XML found for DOI {doi} at BioC-PMC URL {xml_url}.")
            return False,None
        with open(xml_path, "wb+") as f:
            f.write(xml_response.content)
        # logger.info(f"Successfully downloaded XML for DOI {doi} to {xml_path}.")
        return True,xml_url
    except Exception as xml_err:
        # logger.error(f"Failed to download XML from BioC-PMC URL {xml_url}: {xml_err}")
        return False,None


def fallback_elsevier_api(
    paper_metadata: Dict[str, Any], output_path: Path, api_keys: Dict[str, str]
):
    """
    Attempt to download the full text via the Elsevier TDM API.
    For more information, see:
    https://www.elsevier.com/about/policies-and-standards/text-and-data-mining
    (Requires an institutional subscription and an API key provided in the api_keys dictionary under the key "ELSEVIER_TDM_API_KEY".)

    Args:
        paper_metadata (Dict[str, Any]): Dictionary containing paper metadata. Must include the 'doi' key.
        output_path (Path): A pathlib.Path object representing the path where the XML file will be saved.
        api_keys (Dict[str, str]): A dictionary containing API keys. Must include the key "ELSEVIER_TDM_API_KEY".

    Returns:
        bool: True if the XML file was successfully downloaded, False otherwise.
    """
    elsevier_api_key = api_keys.get("ELSEVIER_TDM_API_KEY")
    doi = paper_metadata["doi"]
    api_url = f"https://api.elsevier.com/content/article/doi/{doi}?apiKey={elsevier_api_key}&httpAccept=text%2Fxml"
    logger.info(f"Attempting download via Elsevier API (XML) for {doi}: {api_url}")
    headers = {"Accept": "application/xml"}
    try:
        response = requests.get(api_url, headers=headers, timeout=60)

        # Check for 401 error and look for APIKEY_INVALID in the response
        if response.status_code == 401:
            error_text = response.text
            if "APIKEY_INVALID" in error_text:
                logger.error("Invalid API key. Couldn't download via Elsevier XML API")
            else:
                logger.error("401 Unauthorized. Couldn't download via Elsevier XML API")
            return False

        response.raise_for_status()

        # Attempt to parse it with lxml to confirm it's valid XML
        try:
            etree.fromstring(response.content)
        except etree.XMLSyntaxError as e:
            logger.warning(f"Elsevier API returned invalid XML for {doi}: {e}")
            return False

        xml_path = output_path.with_suffix(".xml")
        with open(xml_path, "wb") as f:
            f.write(response.content)
        logger.info(
            f"Successfully used Elsevier API to downloaded XML for {doi} to {xml_path}"
        )
        return True
    except Exception as e:
        logger.error(f"Could not download via Elsevier XML API: {e}")
        return False


def fallback_elife_xml(doi: str, output_path: Path) :
    """
    Attempt to download the XML via the eLife XML repository on GitHub.

    eLife provides open access to their XML files on GitHub, which can be used as a fallback.
    When multiple versions exist (revised papers), it takes the latest version (e.g., v3 instead of v1).

    Args:
        doi (str): The DOI of the eLife paper to download.
        output_path (Path): A pathlib.Path object representing the path where the XML file will be saved.

    Returns:
        bool: True if the XML file was successfully downloaded, False otherwise.
    """
    parts = doi.split("eLife.")
    if len(parts) < 2:
        logger.error(f"Unable to parse eLife DOI: {doi}")
        return False,None
    article_num = parts[1].strip()

    index = get_elife_xml_index()
    if article_num not in index:
        logger.warning(f"No eLife XML found for DOI {doi}.")
        return False,None
    candidate_files = index[article_num]
    latest_version, latest_download_url = max(candidate_files, key=lambda x: x[0])
    try:
        r = requests.get(latest_download_url, timeout=60)
        r.raise_for_status()
        latest_xml = r.content
    except Exception as e:
        logger.error(f"Error downloading file from {latest_download_url}: {e}")
        return False,None

    xml_path = output_path.with_suffix(".xml")
    with open(xml_path, "wb") as f:
        f.write(latest_xml)
    logger.info(
        f"Successfully downloaded XML via eLife API ({latest_version}) for DOI {doi} to {xml_path}."
    )
    return True,latest_download_url


def get_elife_xml_index():
    """
    Fetch the eLife XML index from GitHub and return it as a dictionary.

    This function retrieves and caches the list of available eLife articles in XML format
    from the eLife GitHub repository. It ensures that the latest version of each article
    is accessible for downloading. The index is cached in memory to avoid repeated
    network requests when processing multiple eLife papers.

    Returns:
        dict: A dictionary where keys are article numbers (as strings) and values are
              lists of tuples (version, download_url). Each list is sorted by version number.
    """
    global ELIFE_XML_INDEX
    if ELIFE_XML_INDEX is None:
        logger.info("Fetching eLife XML index from GitHub using git tree API")
        ELIFE_XML_INDEX = {}
        # Use the git tree API to get the full repository tree.
        base_tree_url = "https://api.github.com/repos/elifesciences/elife-article-xml/git/trees/master?recursive=1"
        r = requests.get(base_tree_url, timeout=60)
        r.raise_for_status()
        tree_data = r.json()
        items = tree_data.get("tree", [])
        # Look for files in the 'articles' directory matching the pattern.
        pattern = r"articles/elife-(\d+)-v(\d+)\.xml"
        for item in items:
            path = item.get("path", "")
            match = re.match(pattern, path)
            if match:
                article_num_padded = match.group(1)
                version = int(match.group(2))
                # Construct the raw download URL.
                download_url = f"https://raw.githubusercontent.com/elifesciences/elife-article-xml/master/{path}"
                ELIFE_XML_INDEX.setdefault(article_num_padded, []).append(
                    (version, download_url)
                )
        # Sort each article's file list by version.
        for key in ELIFE_XML_INDEX:
            ELIFE_XML_INDEX[key].sort(key=lambda x: x[0])
    return ELIFE_XML_INDEX


def month_folder(doi: str):
    """
    Query bioRxiv API to get the posting date of a given DOI.
    Convert a date to the BioRxiv S3 folder name, rolling over if it's the month's last day.
    E.g., if date is the last day of April, treat as May_YYYY.

    Args:
        doi: The DOI for which to retrieve the date.

    Returns:
        Month and year in format `October_2019`
    """
    url = f"https://api.biorxiv.org/details/biorxiv/{doi}/na/json"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    date_str = resp.json()["collection"][0]["date"]
    date = datetime.date.fromisoformat(date_str)

    # NOTE: bioRxiv papers posted on the last day of the month are archived the next day
    last_day = calendar.monthrange(date.year, date.month)[1]
    if date.day == last_day:
        date = date + datetime.timedelta(days=1)
    return date.strftime("%B_%Y")


def list_meca_keys(s3_client, bucket: str, prefix: str):
    """
    List all .meca object keys under a given prefix in a requester-pays bucket.

    Args:
        s3_client: S3 client to get the data from.
        bucket: bucket to get data from.
        prefix: prefix to get data from.

    Returns:
        List of keys, one per existing .meca in the bucket.
    """
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, RequestPayer="requester"
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".meca"):
                keys.append(obj["Key"])
    return keys


def find_meca_for_doi(s3_client, bucket: str, key: str, doi_token: str):
    """
    Efficiently inspect manifest.xml within a .meca zip by fetching only necessary bytes.
    Parse via ZipFile to read manifest.xml and match DOI token.

    Args:
        s3_client: S3 client to get the data from.
        bucket: bucket to get data from.
        key: prefix to get data from.
        doi_token: the DOI that should be matched

    Returns:
        Whether or not the DOI could be matched
    """
    try:
        head = s3_client.get_object(
            Bucket=bucket, Key=key, Range="bytes=0-4095", RequestPayer="requester"
        )["Body"].read()
        tail = s3_client.get_object(
            Bucket=bucket, Key=key, Range="bytes=-4096", RequestPayer="requester"
        )["Body"].read()
    except Exception:
        return False

    data = head + tail
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        manifest = z.read("manifest.xml")

    # Extract the last part of the DOI (newer DOIs that contain date fail otherwise)
    doi_token = doi_token.split('.')[-1]
    return doi_token.encode("utf-8") in manifest.lower()


def fallback_s3(   
    doi: str, output_path: Union[str, Path], api_keys: dict, workers: int = 32
):
    """
    Download a BioRxiv PDF via the requester-pays S3 bucket using range requests.

    Args:
        doi: The DOI for which to retrieve the PDF (e.g. '10.1101/798496').
        output_path: Path where the PDF will be saved (with .pdf suffix added).
        api_keys: Dict containing 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'.

    Returns:
        True if download succeeded, False otherwise.
    """

    s3 = boto3.client(
        "s3",
        aws_access_key_id=api_keys.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=api_keys.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1",
    )
    bucket = "biorxiv-src-monthly"

    # Derive prefix from DOI date
    prefix = f"Current_Content/{month_folder(doi)}/"

    # List MECA archives in that month
    meca_keys = list_meca_keys(s3, bucket, prefix)
    if not meca_keys:
        return False

    token = doi.split("/")[-1].lower()
    target = None
    executor = ThreadPoolExecutor(max_workers=32)
    futures = {
        executor.submit(find_meca_for_doi, s3, bucket, key, token): key
        for key in meca_keys
    }
    target = None
    pbar = tqdm(
        total=len(futures),
        desc=f"Scanning in biorxiv with {workers} workers for {doi}…",
    )
    for future in as_completed(futures):
        key = futures[future]
        try:
            if future.result():
                target = key
                pbar.set_description(f"Success! Found target {doi} in {key}")
                # cancel pending futures to speed shutdown
                for fut in futures:
                    fut.cancel()
                break
        except Exception:
            pass
        finally:
            pbar.update(1)
    # shutdown without waiting for remaining threads
    executor.shutdown(wait=False)
    if target is None:
        logger.error(f"Could not find {doi} on biorxiv")
        return False

    # Download full MECA and extract PDF
    data = s3.get_object(Bucket=bucket, Key=target, RequestPayer="requester")[
        "Body"
    ].read()
    output_path = Path(output_path)
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in z.namelist():
            if name.lower().endswith(".pdf"):
                z.extract(name, path=output_path.parent)
                # Move file to desired location
                (output_path.parent / name).rename(output_path.with_suffix(".pdf"))
                return True
    return False

def fallback_sci_hub(doi: str, output_path: Path, proxies=None):
    """
    Attempt to download the PDF via the Sci-Hub fallback.
    """
    url = "https://www.sci-hub.ren/" + doi + "#"
    head = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36"
    }
    try:
        download_url = ""
        
        # 发送HTTP请求并解析HTML页面
        r = requests.get(url, headers=head,proxies=proxies, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 解析得到文献下载链接
        if soup.iframe == None:
            download_url =soup.embed.attrs["src"] if soup.embed.attrs["src"].startswith("http") else "https:" + soup.embed.attrs["src"]
        else:
            download_url = soup.iframe.attrs["src"] if soup.iframe.attrs["src"].startswith("http") else "https:" + soup.iframe.attrs["src"]
        
        # 下载文献并保存到文件
        # print(doi + "\t正在下载\n下载链接为\t" + download_url)
        download_r = requests.get(download_url, headers=head,proxies=proxies, timeout=20)
        download_r.raise_for_status()
        if download_r.content[:4] != b"%PDF":
            return False,None
        with open(output_path.with_suffix(".pdf"), "wb+") as temp:
            temp.write(download_r.content)
        return True,download_url

    # 下载失败时记录错误信息
    except Exception as e:
        # logger.warning(f"Could not download from: {url} - {e}. ")
        return False,None

def fallback_arxiv(doi: str, output_path: Path,proxies=None):
    """
    Attempt to download the PDF via the Arxiv fallback.
    """
    if "arxiv" in doi:
        doi = doi.split("arxiv.")[1]
        lenth=len(doi.split("."))
        if lenth==1:
            temp=["physics","astro-ph","cond-mat"]
            for i in temp:
                url = f"https://arxiv.org/pdf/{i}/{doi}"
                try:
                    response = requests.get(url,proxies=proxies,timeout=20)
                    response.raise_for_status()
                    if response.content[:4] == b"%PDF":
                        with open(output_path.with_suffix(".pdf"), "wb+") as f:
                            f.write(response.content)
                        return True,url
                except Exception as e:
                    pass
        elif lenth==2:
            url = f"https://arxiv.org/pdf/{doi}"
            try:
                response = requests.get(url,proxies=proxies,timeout=20)
                response.raise_for_status()
                if response.content[:4] == b"%PDF":
                    with open(output_path.with_suffix(".pdf"), "wb+") as f:
                        f.write(response.content)
                    return True,url
            except Exception as e:
                pass
    return False,None

def fallback_doi(doi: str, output_path: Path,proxies=None):
    """
    Attempt to download the PDF via the DOI fallback.
    """
    url = f"https://doi.org/{doi}"
    try:
        response = requests.get(url,proxies=proxies, timeout=20)
        response.raise_for_status()
    except Exception as e:
        return False,None
    soup = BeautifulSoup(response.text, features="lxml")
    meta_pdf = soup.find("meta", {"name": "citation_pdf_url"})
    if meta_pdf and meta_pdf.get("content"):
        pdf_url = meta_pdf.get("content")
        try:
            response = requests.get(pdf_url,proxies=proxies, timeout=20)
            response.raise_for_status()

            if response.content[:4] != b"%PDF" :
                return False,None
            else:
                with open(output_path.with_suffix(".pdf"), "wb+") as f:
                    f.write(response.content)
                return True,pdf_url
        except Exception as e:
            return False,None
    else:
        return False,None


FALLBACKS: Dict[str, Callable] = {
    "bioc_pmc": fallback_bioc_pmc,
    "elife": fallback_elife_xml,
    "elsevier": fallback_elsevier_api,
    "s3": fallback_s3,
    "wiley": fallback_wiley_api,
    "sci_hub": fallback_sci_hub,
    "arxiv": fallback_arxiv,
    "doi": fallback_doi,
}
