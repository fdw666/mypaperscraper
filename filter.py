#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
论文材料领域分类器

功能：
1. 将论文的title发送给本地部署的大模型api
2. 根据文章的标题判断是否是材料领域数据
3. 返回json格式的回复
4. 保存获取的结果并提取材料领域的论文
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from datetime import datetime
from openai import OpenAI
from tqdm import tqdm
from pathlib import Path


class MaterialsClassifier:
    """材料领域论文分类器"""
    
    def __init__(self, base_url: str = "http://localhost:3004/v1", 
                 api_key: str = "dummy", model: str = "Qwen/Qwen2.5-7B-Instruct",
                 max_workers: int = 4):
        """
        初始化分类器
        
        Args:
            base_url: API基础URL
            api_key: API密钥
            model: 模型名称
            max_workers: 最大线程数
        """
        self.client = OpenAI(
            base_url=base_url,
            api_key=api_key
        )
        self.model = model
        self.max_workers = max_workers

    
    def classify_paper(self, title: str,abstract:str) -> bool:
        """
        对单篇论文进行分类
        
        Args:
            title: 论文标题
            abstract: 论文摘要
        Returns:
            分类结果bool
        """
        try:
            # 创建提示词
            prompt = self._create_prompt(title,abstract)
            
            # 调用API
            response = self._call_api(prompt)
            
            # 解析响应
            result = self._parse_response(response)
            
            
            return result
            
        except Exception as e:
            return False
    
    def classify_papers_batch(self, papers: List[Dict]) -> List[Dict]:
        """
        批量分类论文（使用多线程）
        
        Args:
            papers: 论文列表，每个论文包含title和可选的doi
            
        Returns:
            分类结果列表
        """
        
        results = []
        start_time = time.time()
        
        # 使用线程池执行批量分类
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_paper = {}
            for paper in papers:
                title = paper.get('title', '')
                abstract = paper.get('abstract', '')

                if not title and not abstract:
                    continue

                
                future = executor.submit(self.classify_paper, title,abstract)
                future_to_paper[future] = paper
            
            # 收集结果
            
            for future in tqdm(as_completed(future_to_paper),total=len(future_to_paper),desc="分类进度"):
                try:
                    result = future.result()
                    paper=future_to_paper[future]
                    paper['is_materials'] = result
                    results.append(paper)
                    
                    
                        
                except Exception as e:
                    paper = future_to_paper[future]
                    paper['is_materials'] = False
                    results.append(paper)
        
        end_time = time.time()
        print(f"批量分类完成，共处理 {len(results)} 篇论文，耗时 {end_time - start_time:.2f} 秒")
        return results
    
    def _create_prompt(self, title: str,abstract: str) -> str:
        """
        创建分类提示词
        
        Args:
            title: 论文标题
            
        Returns:
            格式化的提示词
        """
        return f"""请判断以下论文标题是否属于材料科学领域。

材料科学包括但不限于以下领域：
- 材料合成与制备
- 材料结构与性能
- 纳米材料
- 复合材料
- 功能材料
- 生物材料
- 能源材料
- 电子材料
- 光学材料
- 磁性材料
- 陶瓷材料
- 金属材料
- 高分子材料
- 材料表征与分析
- 材料计算与模拟

论文标题：{title}
论文摘要：{abstract}

请以JSON格式回复，必须包含以下字段：
- is_materials: 布尔值，表示是否属于材料领域
- confidence: 0-1之间的浮点数，表示判断的置信度
- reasoning: 字符串，说明判断理由

示例回复格式：
{{
    "is_materials": true,
    "confidence": 0.85,
    "reasoning": "该论文涉及纳米材料的合成和性能研究，属于材料科学领域"
}}"""
    
    def _call_api(self, prompt: str) -> str:
        """
        调用大模型API
        
        Args:
            prompt: 提示词
            
        Returns:
            API响应内容
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "你是一个专业的材料科学分类助手。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=500,
                stream=False
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            raise Exception(f"API调用失败: {str(e)}")
    
    def _parse_response(self, content: str) -> bool:
        """
        解析API响应
        
        Args:
            content: API响应内容
        Returns:
            解析后的结果bool
        """
        try:
            result = json.loads(content)
            return result.get('is_materials', False)
        except Exception as e:
            return False
    

#去重并格式化所有doi元数据
#我们需要对的格式为：doi,is_materials,success,title,top_class,main_class,sub_class,type,path,url,date,authors,abstract,journal
def format_and_deduplicate_metadata(metadata_dir: str,output_dir: str,classifier: MaterialsClassifier=None) :
    """
    去重并格式化所有doi元数据
    
    Args:
        metadata_dir: 元数据目录
        output_path: 输出路径
    """
    doi_set=set()
    p=Path(metadata_dir)
    if not p.exists():
        print(f"元数据路径不存在: {metadata_dir}")
        return
    if p.is_file():
        paths=[p]
    else:
        paths=p.rglob("*.jsonl")
    for path in paths:
        results=[]
        top_class=path.parent.parent.name
        main_class=path.parent.name
        sub_class=path.stem
        with open(path, "r", encoding="utf-8") as f:
            for line in f.readlines():
                paper = json.loads(line)
                doi = paper["doi"]
                if not doi or doi in doi_set:
                    continue
                doi_set.add(doi)
                format_paper={
                    "doi": doi,
                    "is_materials": None,
                    "success": None,
                    "title": paper.get("title",None),
                    "top_class": top_class,
                    "main_class": main_class,
                    "sub_class": sub_class,
                    "type": None,
                    "path": None,
                    "url": None,
                    "date": paper.get("date",None),
                    "authors": paper.get("authors",None),
                    "abstract": paper.get("abstract",None),
                    "journal": paper.get("journal",None)
                }
                results.append(format_paper)
        if classifier is not None:
            classify_results=classifier.classify_papers_batch(results)
        else:
            classify_results=results
        output_path=Path(output_dir)/f"{top_class}/{main_class}/{sub_class}.jsonl"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for format_paper in classify_results:
                f.write(json.dumps(format_paper, ensure_ascii=False) + "\n")
    print(f"去重并格式化完成，共处理 {len(doi_set)} 篇论文，保存到 {output_path}")



def main():
    classifier = MaterialsClassifier(
        base_url="http://localhost:3004/v1",
        api_key="dummy",
        model="Qwen/Qwen2.5-7B-Instruct",
        max_workers=4
    )
    doi_dir="/data1/fdwen/pubmed/doi"
    output_dir="/data1/fdwen/pubmed/doi/output"
    format_and_deduplicate_metadata(doi_dir,output_dir,classifier=None)


if __name__ == "__main__":
    main()
