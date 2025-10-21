from my_paperscraper.arxiv import get_and_dump_arxiv_papers
from my_paperscraper.pdf import save_pdf_from_dump_new
from my_paperscraper.pubmed import get_and_dump_pubmed_papers
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import json
from pathlib import Path
import time
import argparse
import signal
from threading import Event
import sys
from multiprocessing import Manager
import keyboard

api_keys={
    "WILEY_TDM_API_TOKEN": None,
    "ELSEVIER_TDM_API_KEY": None,
    "AWS_ACCESS_KEY_ID": None,
    "AWS_SECRET_ACCESS_KEY": None
}
current_word=''
current_pdf_word=''
running=False
running_pdf=False
archive_doi=False
if os.path.exists("record.txt"):
    with open("record.txt", "r", encoding="utf-8") as f:
        current_word=f.read()
else:
    with open("record.txt", "w", encoding="utf-8") as f:
        pass
if os.path.exists("record_pdf.txt"):
    with open("record_pdf.txt", "r", encoding="utf-8") as f:
        current_pdf_word=f.read()
else:
    with open("record_pdf.txt", "w", encoding="utf-8") as f:
        pass

Path("pdf").mkdir(parents=True, exist_ok=True)


def download_doi_parallel(savepath='.'):
    already_downloaded=[]

    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        doipath=Path(savepath)/'doi'
        pdfpath=Path(savepath)/'pdf'
        doipath.mkdir(parents=True, exist_ok=True)
        pdfpath.mkdir(parents=True, exist_ok=True)
        # Path("doi").mkdir(parents=True, exist_ok=True)
        global current_word, running
        with open("target_keys.json", "r", encoding="utf-8") as f:
            keywords_data = json.load(f)
            for category, category_data in keywords_data.items():
                path1=doipath/category
                path2=pdfpath/category
                path1.mkdir(parents=True, exist_ok=True)
                path2.mkdir(parents=True, exist_ok=True)

                if "子类" in category_data:
                    for subcategory, keywords in category_data["子类"].items():
                        

                        if " " in subcategory:
                            subcategory_cn, subcategory_en = subcategory.split(" ", 1)
                        else:
                            subcategory_cn = subcategory
                            subcategory_en = subcategory
                        #这里开始创建doi目录
                        #创建pdf目录
                        path3=path1/subcategory_cn
                        path4=path2/subcategory_cn
                        path3.mkdir(parents=True, exist_ok=True)
                        path4.mkdir(parents=True, exist_ok=True)  
                        for keyword in keywords:
                            if current_word=='':
                                running=True
                            if running:
                                print(f"Download doi of {keyword} in {subcategory_cn}")
                                #获取详情
                                query=[keyword]
                                get_and_dump_pubmed_papers(query, output_filepath=f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl")
                                get_and_dump_arxiv_papers(query, output_filepath=f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl")
                                #保存当前关键词
                                
                                with open("record.txt", "w", encoding="utf-8") as f:
                                    f.write(keyword)
                                current_word=keyword
                                print(f"Success download doi of {keyword}")
                            elif current_word==keyword:
                                running=True
            global archive_doi
            archive_doi=True
            return 'doi下载完成！！！'
                
    except Exception as e:
        print(f"Error in doi: {e}")

def count_keyword():
    count=0
    try:
        with open("target_keys.json", "r", encoding="utf-8") as f:
            keywords_data = json.load(f)
            for category, category_data in keywords_data.items():

                if "子类" in category_data:
                    for subcategory, keywords in category_data["子类"].items():
                        
                        count+=len(keywords)
        print(f"Total keywords: {count}")                
    except Exception as e:
        print(f"Error in doi: {e}")

def download_doi(savepath='.',stop_event:Event=None):
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        doipath=Path(savepath)/'doi'
        pdfpath=Path(savepath)/'pdf'
        doipath.mkdir(parents=True, exist_ok=True)
        pdfpath.mkdir(parents=True, exist_ok=True)
        # Path("doi").mkdir(parents=True, exist_ok=True)
        global current_word, running
        with open("target_keys.json", "r", encoding="utf-8") as f:
            keywords_data = json.load(f)
            for category, category_data in keywords_data.items():
                path1=doipath/category
                path2=pdfpath/category
                path1.mkdir(parents=True, exist_ok=True)
                path2.mkdir(parents=True, exist_ok=True)

                if "子类" in category_data:
                    for subcategory, keywords in category_data["子类"].items():
                        

                        if " " in subcategory:
                            subcategory_cn, subcategory_en = subcategory.split(" ", 1)
                        else:
                            subcategory_cn = subcategory
                            subcategory_en = subcategory
                        #这里开始创建doi目录
                        #创建pdf目录
                        path3=path1/subcategory_cn
                        path4=path2/subcategory_cn
                        path3.mkdir(parents=True, exist_ok=True)
                        path4.mkdir(parents=True, exist_ok=True)  
                        for keyword in keywords:
                            if stop_event.is_set():
                                return 'doi下载完成！！！'
                            if current_word=='':
                                running=True
                            if running:
                                print(f"Download doi of {keyword} in {subcategory_cn}")
                                #获取详情
                                query=[keyword]
                                get_and_dump_pubmed_papers(query, output_filepath=f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl")
                                get_and_dump_arxiv_papers(query, output_filepath=f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl")
                                #保存当前关键词
                                
                                with open("record.txt", "w", encoding="utf-8") as f:
                                    f.write(keyword)
                                current_word=keyword
                                print(f"Success download doi of {keyword}")
                            elif current_word==keyword:
                                running=True
            global archive_doi
            archive_doi=True
            return 'doi下载完成！！！'
                
    except Exception as e:
        print(f"Error in doi: {e}")



def download_pdf(savepath='.',max_workers=5,max_pdf_num=500,ip_pool=None,stop_event:Event=None):
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        doipath=Path(savepath)/'doi'
        pdfpath=Path(savepath)/'pdf'
        doipath.mkdir(parents=True, exist_ok=True)
        pdfpath.mkdir(parents=True, exist_ok=True)
        global current_pdf_word, running_pdf,current_word
        
        with open("target_keys.json", "r", encoding="utf-8") as f:
            keywords_data = json.load(f)
            for category, category_data in keywords_data.items():
                path1=doipath/category
                path2=pdfpath/category
                path1.mkdir(parents=True, exist_ok=True)
                path2.mkdir(parents=True, exist_ok=True)

                if "子类" in category_data:
                    for subcategory, keywords in category_data["子类"].items():
                        

                        if " " in subcategory:
                            subcategory_cn, subcategory_en = subcategory.split(" ", 1)
                        else:
                            subcategory_cn = subcategory
                            subcategory_en = subcategory
                        #这里开始创建doi目录
                        #创建pdf目录
                        path3=path1/subcategory_cn
                        path4=path2/subcategory_cn
                        path3.mkdir(parents=True, exist_ok=True)
                        path4.mkdir(parents=True, exist_ok=True)
                        for keyword in keywords:
                            #给每个关键词创建文件夹
                            path5=path4/keyword.replace(' ', '_')
                            path5.mkdir(parents=True, exist_ok=True)
                            if current_pdf_word=='':
                                running_pdf=True
                            if running_pdf:
                                print(f"Download pdf of {keyword} in {subcategory_cn}")
                                #下载pdf
                                save_pdf_from_dump_new(f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl", pdf_path=f"{path5.as_posix()}",metadata_path=f"{path5.as_posix()}/metadata.jsonl", key_to_save='doi',max_workers=max_workers,max_pdf_num=max_pdf_num,ip_pool=ip_pool,stop_event=stop_event)

                                # #获取详情
                                # get_and_dump_arxiv_papers(query, output_filepath=f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl")
                                #保存当前关键词
                                
                                with open("record_pdf.txt", "w", encoding="utf-8") as f:
                                    f.write(keyword)
                                current_pdf_word=keyword
                                print(f"Success download pdf of {keyword}")
                            if current_pdf_word==keyword:
                                running_pdf=True

            return 'pdf下载完成！！！'
    except Exception as e:
        print(f"Error in pdf: {e}")

def download_pdf_old(savepath='.',max_workers=5,max_pdf_num=500,ip_pool=None,stop_event:Event=None):
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        doipath=Path(savepath)/'doi'
        pdfpath=Path(savepath)/'pdf'
        doipath.mkdir(parents=True, exist_ok=True)
        pdfpath.mkdir(parents=True, exist_ok=True)
        global current_pdf_word, running_pdf,current_word
        
        with open("target_keys.json", "r", encoding="utf-8") as f:
            keywords_data = json.load(f)
            for category, category_data in keywords_data.items():
                path1=doipath/category
                path2=pdfpath/category
                path1.mkdir(parents=True, exist_ok=True)
                path2.mkdir(parents=True, exist_ok=True)

                if "子类" in category_data:
                    for subcategory, keywords in category_data["子类"].items():
                        

                        if " " in subcategory:
                            subcategory_cn, subcategory_en = subcategory.split(" ", 1)
                        else:
                            subcategory_cn = subcategory
                            subcategory_en = subcategory
                        #这里开始创建doi目录
                        #创建pdf目录
                        path3=path1/subcategory_cn
                        path4=path2/subcategory_cn
                        path3.mkdir(parents=True, exist_ok=True)
                        path4.mkdir(parents=True, exist_ok=True)
                        for keyword in keywords:
                            #给每个关键词创建文件夹
                            path5=path4/keyword.replace(' ', '_')
                            path5.mkdir(parents=True, exist_ok=True)
                            if current_pdf_word!='' and current_word=='':

                                raise Exception("current_pdf_word!='' and current_word==''")
                                exit()
                            #到这说明顺序没错
                            #下面检测是否需要等待
                            _archive_doi=archive_doi
                            while current_pdf_word==current_word and not _archive_doi:
                                # running_pdf=False
                                print(f"Waiting for {current_pdf_word} to be different from {current_word}")
                                time.sleep(60)
                                _archive_doi=archive_doi
                                if os.path.exists("record.txt"):
                                    with open("record.txt", "r", encoding="utf-8") as f:
                                        current_word=f.read()
                                else:
                                    with open("record.txt", "w", encoding="utf-8") as f:
                                        pass
                                # exit()
                            #到这说明doi再pdf之前，不用等待
                            #下面检测是否是从当前关键词开始下载pdf
                            if current_pdf_word=='' and current_word!='':
                                running_pdf=True
                            #检测
                            # if current_pdf_word!='':
                            #     running_pdf=True
                            
                            # if current_pdf_word== current_word:
                            #     #沉睡直至不等,挂起
                            #     running_pdf=False
                            if running_pdf:
                                print(f"Download pdf of {keyword} in {subcategory_cn}")
                                #下载pdf
                                save_pdf_from_dump_new(f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl", pdf_path=f"{path5.as_posix()}",metadata_path=f"{path5.as_posix()}/metadata.jsonl", key_to_save='doi',max_workers=max_workers,max_pdf_num=max_pdf_num,ip_pool=ip_pool,stop_event=stop_event)

                                # #获取详情
                                # get_and_dump_arxiv_papers(query, output_filepath=f"{path3.as_posix()}/{keyword.replace(' ', '_')}.jsonl")
                                #保存当前关键词
                                
                                with open("record_pdf.txt", "w", encoding="utf-8") as f:
                                    f.write(keyword)
                                current_pdf_word=keyword
                                print(f"Success download pdf of {keyword}")
                            elif current_pdf_word==keyword:
                                running_pdf=True
            return 'pdf下载完成！！！'
    except Exception as e:
        print(f"Error in pdf: {e}")



if __name__ == "__main__":
    # 创建解析器
    parser = argparse.ArgumentParser(description="爬取pdf")

    # 添加参数
    parser.add_argument("--savepath", type=str, help="存放pdf和doi的位置", default=".")
    parser.add_argument("--maxworkers", type=int, help="线程并行数", default=5)
    parser.add_argument("--maxpdfnum", type=int, help="每个关键词下载pdf的最大数量", default=99999)   

    # 解析参数
    args = parser.parse_args()
    manager = Manager()
    stop_event = manager.Event()
    def handle_sigint():
        stop_event.set()
    keyboard.on_press_key("space", lambda _: handle_sigint())
    # def handle_sigint(sig, frame):
    #     print("\n主进程检测到 Ctrl+C，设置退出标志。")
    #     stop_event.set()
    # signal.signal(signal.SIGINT, handle_sigint)
    
    # with ProcessPoolExecutor(max_workers=2) as executor:
    #     # signal.signal(signal.SIGINT, original_sigint_handler)
    #     try:
    #         future1=executor.submit(download_doi, savepath=args.savepath)
    #         future2=executor.submit(download_pdf, savepath=args.savepath, max_workers=args.maxworkers, max_pdf_num=args.maxpdfnum,stop_event=stop_event)

    #         for future in as_completed([future1,future2]):
    #             print(future.result())
    #     except Exception as e:
    #         stop_event.set()
    # download_doi(savepath=args.savepath,stop_event=stop_event)
    download_pdf(savepath=args.savepath, max_workers=args.maxworkers, max_pdf_num=args.maxpdfnum,stop_event=stop_event)
    print('任务完成！！！')
    

    # # #输出keyword数量
    # # count_keyword()
    # download_doi()
    # download_pdf()
