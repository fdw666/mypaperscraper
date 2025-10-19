"""Functionalities to scrape PDF files of publications."""

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Optional, Union
import threading
import signal

import requests
import tldextract
from bs4 import BeautifulSoup
from tqdm import tqdm

from ..utils import load_jsonl
from .fallbacks import FALLBACKS
from .utils import load_api_keys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

ABSTRACT_ATTRIBUTE = {
    "biorxiv": ["DC.Description"],
    "arxiv": ["citation_abstract"],
    "chemrxiv": ["citation_abstract"],
}
DEFAULT_ATTRIBUTES = ["citation_abstract", "description"]

#需要返回json是否成功success，xml或者pdf:type，路径:path，下载链接:download_url，论文链接:url
def save_pdf(
    paper_metadata: Dict[str, Any],
    filepath: Union[str, Path],
    save_metadata: bool = False,
    api_keys: Optional[Union[str, Dict[str, str]]] = None,
    proxies: Optional[Dict[str, str]] = None,
):
    """
    Save a PDF file of a paper.

    Args:
        paper_metadata: A dictionary with the paper metadata. Must contain the `doi` key.
        filepath: Path to the PDF file to be saved (with or without suffix).
        save_metadata: A boolean indicating whether to save paper metadata as a separate json.
        api_keys: Either a dictionary containing API keys (if already loaded) or a string (path to API keys file).
                  If None, will try to load from `.env` file and if unsuccessful, skip API-based fallbacks.
    """
    if not isinstance(paper_metadata, Dict):
        raise TypeError(f"paper_metadata must be a dict, not {type(paper_metadata)}.")
    if "doi" not in paper_metadata.keys():
        raise KeyError("paper_metadata must contain the key 'doi'.")
    if not isinstance(filepath, str):
        raise TypeError(f"filepath must be a string, not {type(filepath)}.")

    output_path = Path(filepath)

    if not Path(output_path).parent.exists():
        raise ValueError(f"The folder: {output_path} seems to not exist.")

    # load API keys from file if not already loaded via in save_pdf_from_dump (dict)
    if not isinstance(api_keys, dict):
        api_keys = load_api_keys(api_keys)

    doi = paper_metadata["doi"]
    outputtype=None
    url =None
    success = False
    paper_metadata["success"] = success
    paper_metadata["type"] = outputtype
    paper_metadata["path"] = None
    paper_metadata["url"] = None
    
    if not success and "arxiv" in doi:
        success,url = FALLBACKS["arxiv"](doi, output_path, proxies=proxies)
        if success:
            outputtype="pdf"
    if not success:
        success,url = FALLBACKS["doi"](doi, output_path, proxies=proxies)
        if success:
            outputtype="pdf"
    #从sci-hub下载
    if not success:
        success,url = FALLBACKS["sci_hub"](doi, output_path, proxies=proxies)
        if success:
            outputtype="pdf"
            
    if not success:
        # always first try fallback to BioC-PMC (open access papers on PubMed Central)
        success,url = FALLBACKS["bioc_pmc"](doi, output_path, proxies=proxies)
        if success:
            outputtype="xml"
    paper_metadata["success"] = success
    paper_metadata["type"] = outputtype
    paper_metadata["url"] = url
    if success:
        paper_metadata["path"] = output_path.with_suffix(f".{outputtype}").as_posix()
    return paper_metadata


#需要返回json是否成功success，xml或者pdf:type，路径:path，下载链接:download_url，论文链接:url
def save_pdf_old(
    paper_metadata: Dict[str, Any],
    filepath: Union[str, Path],
    save_metadata: bool = False,
    api_keys: Optional[Union[str, Dict[str, str]]] = None,
    proxies: Optional[Dict[str, str]] = None,
):
    """
    Save a PDF file of a paper.

    Args:
        paper_metadata: A dictionary with the paper metadata. Must contain the `doi` key.
        filepath: Path to the PDF file to be saved (with or without suffix).
        save_metadata: A boolean indicating whether to save paper metadata as a separate json.
        api_keys: Either a dictionary containing API keys (if already loaded) or a string (path to API keys file).
                  If None, will try to load from `.env` file and if unsuccessful, skip API-based fallbacks.
    """
    if not isinstance(paper_metadata, Dict):
        raise TypeError(f"paper_metadata must be a dict, not {type(paper_metadata)}.")
    if "doi" not in paper_metadata.keys():
        raise KeyError("paper_metadata must contain the key 'doi'.")
    if not isinstance(filepath, str):
        raise TypeError(f"filepath must be a string, not {type(filepath)}.")

    output_path = Path(filepath)

    if not Path(output_path).parent.exists():
        raise ValueError(f"The folder: {output_path} seems to not exist.")

    # load API keys from file if not already loaded via in save_pdf_from_dump (dict)
    if not isinstance(api_keys, dict):
        api_keys = load_api_keys(api_keys)

    doi = paper_metadata["doi"]
    outputtype="pdf"
    url = f"https://doi.org/{doi}"#https://doi.org/10.1007/s10653-024-02017-z
    success = False
    paper_metadata["success"] = success
    paper_metadata["type"] = None
    paper_metadata["path"] = None
    paper_metadata["url"] = None
    try:
        response = requests.get(url,proxies=proxies, timeout=20)
        response.raise_for_status()
        success = True
    except Exception as e:
        error = str(e)
        logger.warning(f"Could not download from: {url} - {e}. ")
    #未处理
    if not success and "biorxiv" in error:
        if (
            api_keys.get("AWS_ACCESS_KEY_ID") is None
            or api_keys.get("AWS_SECRET_ACCESS_KEY") is None
        ):
            logger.info(
                "BiorXiv PDFs can be downloaded from a S3 bucket with a requester-pay option. "
                "Consider setting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to use this option. "
                "Pricing is a few cent per GB, thus each request costs < 0.1 cents. "
                "For details see: https://www.biorxiv.org/tdm"
            )
        else:
            success = FALLBACKS["s3"](doi, output_path, api_keys)
            if success:
                return paper_metadata
    if not success and "arxiv" in doi:
        success,temp_url = FALLBACKS["arxiv"](doi, output_path, proxies=proxies)
        if success:
            url = temp_url
            outputtype="pdf"
            paper_metadata["success"] = success
            paper_metadata["type"] = outputtype
            paper_metadata["path"] = output_path.with_suffix(f".{outputtype}").as_posix()
            paper_metadata["url"] = url
            return paper_metadata
    #从sci-hub下载
    if not success:
        success,temp_url = FALLBACKS["sci_hub"](doi, output_path, proxies=proxies)
        if success:
            url = temp_url
            outputtype="pdf"
            paper_metadata["success"] = success
            paper_metadata["type"] = outputtype
            paper_metadata["path"] = output_path.with_suffix(f".{outputtype}").as_posix()
            paper_metadata["url"] = url
            return paper_metadata
            
    if not success:
        # always first try fallback to BioC-PMC (open access papers on PubMed Central)
        success,temp_url = FALLBACKS["bioc_pmc"](doi, output_path, proxies=proxies)
        if success:
            url = temp_url
            outputtype="xml"
            paper_metadata["success"] = success
            paper_metadata["type"] = outputtype
            paper_metadata["path"] = output_path.with_suffix(f".{outputtype}").as_posix()
            paper_metadata["url"] = url
            return paper_metadata

        # if BioC-PMC fails, try other fallbacks
        if not success:
            # check for specific publishers
            if "elife" in error.lower():  # elife has an open XML repository on GitHub
                success,temp_url = FALLBACKS["elife"](doi, output_path)
                if success:
                    url = temp_url
                    outputtype="xml"
                    paper_metadata["success"] = success
                    paper_metadata["type"] = outputtype
                    paper_metadata["path"] = output_path.with_suffix(f".{outputtype}").as_posix()
                    paper_metadata["url"] = url
                    return paper_metadata
            elif (
                ("wiley" in error.lower())
                and api_keys
                and ("WILEY_TDM_API_TOKEN" in api_keys)
            ):
                FALLBACKS["wiley"](paper_metadata, output_path, api_keys)
        return paper_metadata

    soup = BeautifulSoup(response.text, features="lxml")
    meta_pdf = soup.find("meta", {"name": "citation_pdf_url"})
    if meta_pdf and meta_pdf.get("content"):
        pdf_url = meta_pdf.get("content")
        try:
            response = requests.get(pdf_url,proxies=proxies, timeout=20)
            response.raise_for_status()

            if response.content[:4] != b"%PDF" :
                logger.warning(
                    f"The file from {url} does not appear to be a valid PDF."
                )
                success,temp_url = FALLBACKS["sci_hub"](doi, output_path,proxies=proxies)
                if success:
                    url = temp_url
                    outputtype="pdf"
                if not success:
                    success,temp_url = FALLBACKS["bioc_pmc"](doi, output_path,proxies=proxies)
                    if success:
                        url = temp_url
                        outputtype="xml"
                if not success:
                    # Check for specific publishers
                    if "elife" in doi.lower():
                        logger.info("Attempting fallback to eLife XML repository")
                        success,temp_url = FALLBACKS["elife"](doi, output_path)
                        if success:
                            url = temp_url
                            outputtype="xml"
                            
                    elif api_keys and "WILEY_TDM_API_TOKEN" in api_keys:
                        FALLBACKS["wiley"](paper_metadata, output_path, api_keys)
                    elif api_keys and "ELSEVIER_TDM_API_KEY" in api_keys:
                        FALLBACKS["elsevier"](paper_metadata, output_path, api_keys)
            else:
                with open(output_path.with_suffix(".pdf"), "wb+") as f:
                    f.write(response.content)
                outputtype="pdf"
                success = True
                url = pdf_url
        except Exception as e:
            logger.warning(f"Could not download {pdf_url}: {e}")
    else:  # if no citation_pdf_url meta tag found, try other fallbacks
        success,temp_url = FALLBACKS["sci_hub"](doi, output_path,proxies=proxies)
        if success:
            url = temp_url
            outputtype="pdf"
        elif "elife" in doi.lower():
            logger.info(
                "DOI contains eLife, attempting fallback to eLife XML repository on GitHub."
            )
            success,temp_url = FALLBACKS["elife"](doi, output_path)
            if success:
                url = temp_url
                outputtype="xml"
                
            else:
                logger.warning(
                    f"eLife XML fallback failed for {paper_metadata['doi']}."
                )
        elif (
            api_keys and "ELSEVIER_TDM_API_KEY" in api_keys
        ):  # elsevier journals can be accessed via the Elsevier TDM API (requires API key)
            FALLBACKS["elsevier"](paper_metadata, output_path, api_keys)
        else:
            logger.warning(
                f"Retrieval failed. No citation_pdf_url meta tag found for {url} and no applicable fallback mechanism available."
            )
    paper_metadata["success"] = success
    paper_metadata["type"] = outputtype
    paper_metadata["path"] = output_path.with_suffix(f".{outputtype}").as_posix()
    paper_metadata["url"] = url
    return paper_metadata

    if not save_metadata:
        return paper_metadata

    metadata = {}
    # Extract title
    title_tag = soup.find("meta", {"name": "citation_title"})
    metadata["title"] = title_tag.get("content") if title_tag else "Title not found"

    # Extract authors
    authors = []
    for author_tag in soup.find_all("meta", {"name": "citation_author"}):
        if author_tag.get("content"):
            authors.append(author_tag["content"])
    metadata["authors"] = authors if authors else ["Author information not found"]

    # Extract abstract
    domain = tldextract.extract(url).domain
    abstract_keys = ABSTRACT_ATTRIBUTE.get(domain, DEFAULT_ATTRIBUTES)

    for key in abstract_keys:
        abstract_tag = soup.find("meta", {"name": key})
        if abstract_tag:
            raw_abstract = BeautifulSoup(
                abstract_tag.get("content", "None"), "html.parser"
            ).get_text(separator="\n")
            if raw_abstract.strip().startswith("Abstract"):
                raw_abstract = raw_abstract.strip()[8:]
            metadata["abstract"] = raw_abstract.strip()
            break

    if "abstract" not in metadata.keys():
        metadata["abstract"] = "Abstract not found"
        logger.warning(f"Could not find abstract for {url}")
    elif metadata["abstract"].endswith("..."):
        logger.warning(f"Abstract truncated from {url}")

    # Save metadata to JSON
    try:
        with open(output_path.with_suffix(".json"), "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Failed to save metadata to {str(output_path)}: {e}")


def _process_single_paper(args):
    """
    处理单个论文下载的辅助函数，用于多线程执行
    
    Args:
        args: 包含 (paper, pdf_path, key_to_save, save_metadata, api_keys) 的元组
    
    Returns:
        tuple: (success, paper_title, message)
    """
    paper, pdf_path, key_to_save, save_metadata, api_keys,ip_pool = args
    
    try:
        if paper["success"] or paper["is_materials"] is False:
            return None
        paper["success"] = False
        paper["type"] = None
        paper["path"] = None
        paper["url"] = None
        if "doi" not in paper.keys() or paper["doi"] is None:
            return None
        
        filename = paper[key_to_save].replace("/", "_")
        pdf_file = Path(os.path.join(pdf_path, f"{filename}.pdf"))
        xml_file = pdf_file.with_suffix(".xml")
        

        #去重
        for subdir in Path(pdf_path).parent.iterdir():
            if subdir.is_dir():
                file_path_pdf=subdir/f"{filename}.pdf"
                file_path_xml=subdir/f"{filename}.xml"
                if file_path_pdf.exists():
                    paper["success"] = True
                    paper["type"] = "pdf"
                    paper["path"] = file_path_pdf.as_posix()
                    return paper
                if file_path_xml.exists():
                    paper["success"] = True
                    paper["type"] = "xml"
                    paper["path"] = file_path_xml.as_posix()
                    return paper
        
        
        output_path = str(pdf_file)
        # 使用IP池获取IP
        if ip_pool:
            proxies = ip_pool.get_proxies()
            if not proxies:
                metadata=save_pdf(paper, output_path, save_metadata=save_metadata, api_keys=api_keys)
            else:
                # 设置代理
                metadata=save_pdf(paper, output_path, save_metadata=save_metadata, api_keys=api_keys,proxies=proxies)
        else:
            metadata=save_pdf(paper, output_path, save_metadata=save_metadata, api_keys=api_keys)
        return metadata
        
    except Exception as e:
        return paper



def save_pdf_from_dump_new(
    dump_path: str,
    pdf_path: str,
    metadata_path: str = None,
    key_to_save: str = "doi",
    save_metadata: bool = False,
    api_keys: Optional[Union[str, Dict[str, str]]] = None,
    max_workers: int = 10,
    max_pdf_num: int = 500,
    ip_pool = None,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """
    Receives a path to a `.jsonl` dump with paper metadata and saves the PDF files of
    each paper using multi-threading for improved performance.

    Args:
        dump_path: Path to a `.jsonl` file with paper metadata, one paper per line.
        pdf_path: Path to a folder where the files will be stored.
        key_to_save: Key in the paper metadata to use as filename.
            Has to be `doi` or `title`. Defaults to `doi`.
        save_metadata: A boolean indicating whether to save paper metadata as a separate json.
        api_keys: Path to a file with API keys. If None, API-based fallbacks will be skipped.
        max_workers: Maximum number of worker threads. Defaults to 10.
        stop_event: Event object to signal the stop of the program.
    """

    if not isinstance(dump_path, str):
        raise TypeError(f"dump_path must be a string, not {type(dump_path)}.")
    if not dump_path.endswith(".jsonl"):
        raise ValueError("Please provide a dump_path with .jsonl extension.")

    if not isinstance(pdf_path, str):
        raise TypeError(f"pdf_path must be a string, not {type(pdf_path)}.")

    if not isinstance(key_to_save, str):
        raise TypeError(f"key_to_save must be a string, not {type(key_to_save)}.")
    if key_to_save not in ["doi", "title", "date"]:
        raise ValueError("key_to_save must be one of 'doi' or 'title'.")

    # papers = load_jsonl(dump_path)
    metadict={}
    if os.path.exists(dump_path):
        with open(dump_path, "r", encoding="utf-8") as f:
            #字典列表
            for line in f.readlines():
                paper = json.loads(line)
                metadict[paper["doi"]] = paper

    if not isinstance(api_keys, dict):
        api_keys = load_api_keys(api_keys)
    #去重doi
    # doi_set = set()
    # for paper in papers:
    #     if paper["doi"] not in doi_set:
    #         doi_set.add(paper["doi"])
    #     else:
    #         papers.remove(paper)
    

    # 准备任务参数
    tasks = [
        (paper, pdf_path, key_to_save, save_metadata, api_keys,ip_pool) 
        for paper in metadict.values() if paper["is_materials"] and not paper["success"]
    ]
    
    # 使用多线程执行下载任务
    # stop_event=threading.Event()
    #先读取metadata_path文件，如果存在，则读取文件内容

    # with open(metadata_path, "a", encoding="utf-8") as f:
    def f_save_metadata(signal=None, frame=None):
        print("检测到终端信号，保存metadata")
        with open(dump_path, "w", encoding="utf-8") as f:
            for metadata in metadict.values():
                f.write(json.dumps(metadata, ensure_ascii=False) + "\n")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_paper = {
            executor.submit(_process_single_paper, task): task[0] 
            for task in tasks
        }
        
        # 使用tqdm显示进度
        with tqdm(total=len(tasks), desc="Downloading PDFs") as pbar:
            for future in as_completed(future_to_paper):
                if future.cancelled():
                    continue
                paper = future_to_paper[future]
                
                try:
                    
                    metadata = future.result()
                    # if stop_event.is_set():
                    #     executor.shutdown(wait=False)
                    if metadata is None:
                        logger.warning(f"without doi or already exists")
                        pbar.update(1)
                        continue
                    #记录metadata
                    metadict[metadata["doi"]] = metadata
                    # f.write(json.dumps(metadata, ensure_ascii=False) + "\n")
                    if metadata["success"]:
                        pass
                        # logger.info(f"Successfully download {metadata["type"]} of {metadata["doi"]}")
                    else:
                        logger.warning(f"Failed to download  {metadata["doi"]}")
                except Exception as e:
                    logger.error(f"Exception occurred while downloading {metadata["doi"]}: {str(e)}")
                
                pbar.update(1)
                #计算该关键词下载了多少pdf#如果超过500就停止下载，结束线程
                if len(os.listdir(pdf_path))>max_pdf_num or stop_event.is_set():
                    # stop_event.set()
                    for f in future_to_paper:
                        if not f.done():
                            f.cancel()
                    # executor.shutdown(wait=False)
                    
        #保存metadata
        f_save_metadata(None, None)
        if stop_event.is_set():
            exit()
        # with open(metadata_path, "w", encoding="utf-8") as f:
        #     for doi, metadata in metadict.items():
        #         #修改，加入顺序调整
        #         order_metadata = {}
        #         order_metadata["doi"] = doi
        #         order_metadata["success"] = getattr(metadata, "success",False)
        #         order_metadata["title"] = getattr(metadata, "title",None)
        #         order_metadata["top_class"] = getattr(metadata, "top_class",None)
        #         order_metadata["main_class"] = getattr(metadata, "main_class",None)
        #         order_metadata["sub_class"] = getattr(metadata, "sub_class",None)
        #         order_metadata["type"] = getattr(metadata, "type",None)
        #         order_metadata["path"] = getattr(metadata, "path",None)
        #         order_metadata["url"] = getattr(metadata, "url",None)
        #         order_metadata["metric"] = getattr(metadata, "metric",False)
        #         order_metadata["date"] = getattr(metadata, "date",None)
        #         order_metadata["authors"] = getattr(metadata, "authors",None)
        #         order_metadata["abstract"] = getattr(metadata, "abstract",None)
        #         order_metadata["journal"] = getattr(metadata, "journal",None)
        #         order_metadata.update(metadata)
        #         f.write(json.dumps(order_metadata, ensure_ascii=False) + "\n")

                


def save_pdf_from_dump(
    dump_path: str,
    pdf_path: str,
    key_to_save: str = "doi",
    save_metadata: bool = False,
    api_keys: Optional[str] = None,
) -> None:
    """
    Receives a path to a `.jsonl` dump with paper metadata and saves the PDF files of
    each paper.

    Args:
        dump_path: Path to a `.jsonl` file with paper metadata, one paper per line.
        pdf_path: Path to a folder where the files will be stored.
        key_to_save: Key in the paper metadata to use as filename.
            Has to be `doi` or `title`. Defaults to `doi`.
        save_metadata: A boolean indicating whether to save paper metadata as a separate json.
        api_keys: Path to a file with API keys. If None, API-based fallbacks will be skipped.
    """

    if not isinstance(dump_path, str):
        raise TypeError(f"dump_path must be a string, not {type(dump_path)}.")
    if not dump_path.endswith(".jsonl"):
        raise ValueError("Please provide a dump_path with .jsonl extension.")

    if not isinstance(pdf_path, str):
        raise TypeError(f"pdf_path must be a string, not {type(pdf_path)}.")

    if not isinstance(key_to_save, str):
        raise TypeError(f"key_to_save must be a string, not {type(key_to_save)}.")
    if key_to_save not in ["doi", "title", "date"]:
        raise ValueError("key_to_save must be one of 'doi' or 'title'.")

    papers = load_jsonl(dump_path)

    if not isinstance(api_keys, dict):
        api_keys = load_api_keys(api_keys)

    pbar = tqdm(papers, total=len(papers), desc="Processing")
    for i, paper in enumerate(pbar):
        pbar.set_description(f"Processing paper {i + 1}/{len(papers)}")

        if "doi" not in paper.keys() or paper["doi"] is None:
            logger.warning(f"Skipping {paper['title']} since no DOI available.")
            continue
        filename = paper[key_to_save].replace("/", "_")
        pdf_file = Path(os.path.join(pdf_path, f"{filename}.pdf"))
        xml_file = pdf_file.with_suffix(".xml")
        if pdf_file.exists():
            logger.info(f"File {pdf_file} already exists. Skipping download.")
            continue
        if xml_file.exists():
            logger.info(f"File {xml_file} already exists. Skipping download.")
            continue
        output_path = str(pdf_file)
        save_pdf(paper, output_path, save_metadata=save_metadata, api_keys=api_keys)
