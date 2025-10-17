import requests
from bs4 import BeautifulSoup
from pathlib import Path
import os

# url="https://doi.org/10.1007/s10653-024-02017-z"
# response = requests.get(url, timeout=20)
# response.raise_for_status()



# 请求头
head = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36",
     "Accept": "application/pdf,*/*;q=0.9"
}
doi="10.3791/54551"
output_path=Path("test.xml")

def fallback_bioc_pmc(doi: str, output_path: Path,proxies=None) -> bool:

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
        print(data)
        records = data.get("records", [])
        if not records or "pmcid" not in records[0]:
            print("No PMCID available for DOI {doi}. Fallback via PMC therefore not possible.")
            return False
        pmcid = records[0]["pmcid"]
        print(pmcid)
        pmid = records[0]["pmid"]
    except Exception as conv_err:
        print(conv_err)
        return False
    # return True
    # Construct PMC XML URL
    #现在已经获取id

    # 首先下载pdf  "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_xml/{pmcid}/unicode"
    url = 'https://www.ncbi.nlm.nih.gov/pmc/articles/' + str(pmcid) + '/'
    try:
        headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36",
                   "Accept": "application/pdf,*/*;q=0.9"}
        request = requests.get(url, headers=headers,proxies=proxies,timeout=20)
        with open("test.html", "wb") as f:
            f.write(request.content)
        bs = BeautifulSoup(request.content, 'lxml')
        download_url = bs.find(attrs={'name':'citation_pdf_url'}).get('content')
        print(download_url)
        r = requests.get(download_url, headers=headers,proxies=proxies,timeout=20)
        if r.content[:4] != b"%PDF":
            # print(r.content)
            raise Exception("Not a valid PDF")
        with open(output_path.with_suffix(".pdf"), 'wb') as f:
            f.write(r.content)
        return True
    except Exception:
        pass
    #如果不行，就下载xml
    
    # xml_url = f"https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_xml/{pmcid}/unicode"
    # try:
    #     xml_response = requests.get(xml_url, proxies=proxies,timeout=20)
    #     xml_response.raise_for_status()
    #     xml_path = output_path.with_suffix(".xml")
    #     # check for xml error:
    #     if xml_response.content.startswith(
    #         b"[Error] : No result can be found. <BR><HR><B> - https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/"
    #     ):
    #         return False
    #     with open(xml_path, "wb+") as f:
    #         f.write(xml_response.content)
    #     return True
    # except Exception as xml_err:
    #     return False
# fallback_bioc_pmc(doi, output_path)
# 下载文献的函数
def download_paper(doi):
    # 拼接Sci-Hub链接
    url = "https://www.sci-hub.ren/" + doi + "#"
    
    try:
        download_url = ""
        
        # 发送HTTP请求并解析HTML页面
        r = requests.get(url, headers=head)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 解析得到文献下载链接
        if soup.iframe == None:
            download_url =  soup.embed.attrs["src"]
        else:
            download_url = soup.iframe.attrs["src"]
        
        # 下载文献并保存到文件
        print(doi + "\t正在下载\n下载链接为\t" + download_url)
        download_r = requests.get(download_url, headers=head)
        download_r.raise_for_status()
        with open("test.pdf", "wb+") as temp:
            temp.write(download_r.content)

        print(doi + "\t文献下载成功.\n")

    # 下载失败时记录错误信息
    except Exception as e:
        print(e)
download_paper(doi)
def download_pdf(doi):
    url = f"https://doi.org/{doi}"
    try:
        response = requests.get(url,headers=head,timeout=20)
        response.raise_for_status()
        
    except Exception as e:
        print(e)
    soup = BeautifulSoup(response.text, features="lxml")
    meta_pdf = soup.find("meta", {"name": "citation_pdf_url"})
    if meta_pdf and meta_pdf.get("content"):
        pdf_url = meta_pdf.get("content")
        try:
            response = requests.get(pdf_url, headers=head,timeout=20)
            response.raise_for_status()
            if response.content[:4] != b"%PDF":
                print("Not a valid PDF")

            with open("test.pdf", "wb+") as temp:
                temp.write(response.content)
        except Exception as e:
            print(e)
# download_pdf(doi)

def download_arxiv_pdf(doi):
    if "arxiv" in doi:
        doi = doi.split("arxiv.")[1]
        lenth=len(doi.split("."))
        if lenth==1:
            temp=["physics","astro-ph","cond-mat"]
            for i in temp:
                url = f"https://arxiv.org/pdf/{i}/{doi}"
                try:
                    response = requests.get(url,timeout=20)
                    response.raise_for_status()
                    if response.content[:4] == b"%PDF":
                        with open("test.pdf", "wb+") as f:
                            f.write(response.content)
                        return
                except Exception as e:
                    print(e)
        elif lenth==2:
            url = f"https://arxiv.org/pdf/{doi}"
            try:
                response = requests.get(url,timeout=20)
                response.raise_for_status()
                if response.content[:4] == b"%PDF":
                    with open("test.pdf", "wb+") as f:
                        f.write(response.content)
                    return
            except Exception as e:
                print(e)
        else:
            pass
    else:
        pass

# download_arxiv_pdf(doi)
# dict1={"1":123,"2":456,"3":789}
# dict2={"4":123,"3":456,"2":789}
# dict1.update(dict2)
# print(dict1)






