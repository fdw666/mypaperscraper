from pathlib import Path
import json
from typing import Union
def update_metadata(metadata_dir: str,pdf_dir: Union[str,list[str]]):
    if isinstance(pdf_dir, str):
        pdf_dir=[pdf_dir]
    doi_path_dict={}
    for pdf_path in pdf_dir:
        pdf_path=Path(pdf_path)
        if not pdf_path.exists():
            print(f"PDF路径不存在: {pdf_path}")
            continue
        if pdf_path.is_dir():
            xml_paths=pdf_path.rglob("*.xml")
            pdf_paths=pdf_path.rglob("*.pdf")
            for xml_path in xml_paths:
                doi=xml_path.stem.replace("_","/")
                doi_path_dict[doi]=xml_path.as_posix()
            for pdf_path in pdf_paths:
                doi=pdf_path.stem.replace("_","/")
                doi_path_dict[doi]=pdf_path.as_posix()
    #读取metadata_dir下的所有jsonl文件
    metadata_paths=Path(metadata_dir).rglob("*.jsonl")
    for metadata_path in metadata_paths:
        results=[]
        with open(metadata_path, "r", encoding="utf-8") as f:
            for line in f.readlines():
                paper=json.loads(line)
                doi=paper["doi"]
                if doi in doi_path_dict:
                    paper["path"]=doi_path_dict[doi]
                    # paper["url"]=doi_path_dict[doi]
                    paper["success"]=True
                    paper["type"]=doi_path_dict[doi].split(".")[-1] if "." in doi_path_dict[doi] else None
                results.append(paper)
        with open(metadata_path, "w", encoding="utf-8") as f:
            for paper in results:
                f.write(json.dumps(paper, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    metadata_dir="/data1/fdwen/pubmed/doi/output"
    pdf_dir="/data1/fdwen/pubmed/pdf"
    update_metadata(metadata_dir,pdf_dir)