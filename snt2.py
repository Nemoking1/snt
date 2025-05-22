from datetime import datetime
import os
import concurrent.futures
from pathlib import Path
from collections import defaultdict
from openpyxl import load_workbook
from sinotrans.core import FileParser, ExcelProcessor
from sinotrans.utils import Logger, GlobalThreadPool, ProgressManager
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

class AutoSntProcessor:
    # 预定义配置
    DEFAULT_FALLBACK_SHEETS = ["Sheet1"]
    # 用于关联不同文件中的行数据，表中必须存在的字段
    KEY_FIELDS = ["folder","po","lot"]
    # 用于检验表中数据的有效性，通常和strice_flag配合使用
    REQUIRED_FIELDS = ["fwd_feedback","REMARK"] # "folder","lot"
    # 用于获取句柄时，检查表中是否存在有效表，sheet_names是其他文件的有效表，REPORT略不一样
    # REPORT_SHEETS = ["Follow UP"]
    def __init__(self):
        # 初始化路径配置
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self._init_paths()
        self._init_logger()
        self._init_thread_pool()

    def _init_paths(self):
        """初始化所有路径配置"""
        self.target_path = os.path.join(self.current_dir, "target")
        self.config_path = os.path.join(self.current_dir, "conf")
        self.snt_path = os.path.join(self.current_dir, "snt")
        # self.report_path = os.path.join(self.current_dir, "report")
        self.response_path = os.path.join(self.current_dir, "res")
        
        self.template_file = os.path.join(self.current_dir, "template.xlsx")
        self.target_file = os.path.join(self.target_path, f"output_{self.timestamp}.xlsx")

        self.sheet_config_file = os.path.join(self.config_path, "sheet_config.txt")
        self.fixed_mapping_file = os.path.join(self.config_path, "fixed_mapping.txt")
        #self.bc4_report_mapping_file = os.path.join(self.config_path, "bc4_report_mapping.txt")
        self.pending_po_mapping_file = os.path.join(self.config_path, "pending_po_mapping.txt")
        self.response_mapping_file = os.path.join(self.config_path, "response_mapping.txt")
        self.target_file = os.path.join(self.target_path, f"output_{self.timestamp}.xlsx")
        
        FileParser.ensure_directories_exist([
            self.target_path, self.config_path,
            self.snt_path, self.response_path # , self.report_path
        ])

    def _init_logger(self):
        """初始化日志系统"""
        debug_path = os.path.join(self.current_dir, "logs")
        Logger(debug_path=debug_path)

    def _init_thread_pool(self):
        """初始化全局线程池"""
        GlobalThreadPool.initialize(
            max_workers=16,
            thread_name_prefix='AutoSNTThreadPool'
        )

    def _load_mappings(self):
        """加载所有映射配置"""
        try:
            self.sheet_names = FileParser.parse_conf(self.sheet_config_file, ',')
            self.fixed_mapping = FileParser.parse_mapping_dict(self.fixed_mapping_file,':', '|', ',', '=')   # 模板值映射
            self.snt_mapping = FileParser.parse_mapping_dict_of_list(self.pending_po_mapping_file,':', '|', ',', '=')
            self.response_mapping = FileParser.parse_mapping_dict_of_list(self.response_mapping_file,':', '|', ',', '=')
            #self.bc4_report_mapping = FileParser.parse_mapping_dict_of_list(self.bc4_report_mapping_file,':', '|', ',', '=')
            
            Logger.info("✅ 映射文件加载成功")
        except Exception as e:
            Logger.error(f"❌ 映射文件加载失败: {str(e)}")
            raise

    def _validate_input_files(self):
        """验证输入文件完整性"""
        try:
            self.snt_files = FileParser.read_files(self.snt_path, [".xlsx", ".xls"])
            # self.report_files = FileParser.read_files(self.report_path, [".xlsx", ".xls"])
            self.response_files = FileParser.read_files(self.response_path, [".xlsx", ".xls"])
            
            # 校验所有文件的工作表结构
            all_files = self.snt_files + self.response_files
            # 预设模板文件检查（保持严格校验）
            self.sheet_maps = ExcelProcessor.get_workbook_sheets(
                file_paths=all_files,
                preset_sheets=self.sheet_names,  # 用于生成警告信息
                read_only=True,
                verbose=False
            )


            # # 特殊处理report_files（动态加载所有实际sheet）
            # report_sheets = ExcelProcessor.check_excel_sheets(
            #     self.report_files,
            #     preset_sheets=self.REPORT_SHEETS,
            # )
            # report_sheets = ExcelProcessor.get_workbook_sheets(
            #     file_paths=self.report_files,
            #     preset_sheets=self.sheet_names,  # 用于生成警告信息
            #     read_only=True,
            #     verbose=False
            # )

            # 合并映射表（report_files覆盖同名文件）
            # self.sheet_maps.update(report_sheets)
            Logger.info("✅ 文件验证通过")
        except Exception as e:
            Logger.error(f"❌ 文件验证失败: {str(e)}")
            raise
    def _process_single_sheet(self, sheet_name, output_wb):
        """处理单个工作表"""
        try:
            new_rows = []
            output_ws = output_wb[sheet_name]
            headers = [cell.value for cell in output_ws[1]]
            Logger.info(f"🔨 开始处理工作表 [{sheet_name}]")
            progress = ProgressManager()
            # ----------------------------
            # 阶段一：加载SNT基准数据到内存
            # ----------------------------
            # 查找snt文件夹文件
            snt_file = next((fp for fp in self.sheet_maps.keys() if "snt" in fp.lower()), None)
            if not snt_file:
                raise ValueError("未找到snt文件夹下的基准文件")

            # 加载snt数据到内存字典{key:row}
            snt_data = {}
            snt_ws = self.sheet_maps[snt_file][sheet_name]
            snt_gen = ExcelProcessor.excel_row_generator(
                snt_ws,
                snt_file,
                progress,
                self.KEY_FIELDS,
                strict_flag=False
            )
            for row in snt_gen:
                key = tuple(row[field] for field in self.KEY_FIELDS)
                if key in snt_data:
                    Logger.info(f"❌ 发现重复基准数据: {key}")
                snt_data[key] = row

            base_data = {}
            for key, snt_row in snt_data.items():
                # 转换SNT数据到目标列格式
                base_row = {header: '' for header in headers}
                base_row.update(ExcelProcessor.column_mapping(snt_row, self.snt_mapping))
                base_data[key] = base_row

            progress.close()
            Logger.info(f"📥 已加载 {len(snt_data)} 条有效基准数据")

            # -----------------------------------------------------------------
            # 阶段二：处理其他文件夹数据，以文件夹为读取单位，以sheet_name为写入单位
            # -----------------------------------------------------------------
            folder_sources = defaultdict(list)
            # 设置进度条
            total_rows = 0
            for fp in self.sheet_maps.keys():
                if fp == snt_file:
                    continue  # 跳过snt文件
                
                file_type = self._get_folder_type(fp)
                # 保存{文件类型：文件路径}，sheet_maps保存{文件路径：{工作表名称：工作表对象}，......}
                folder_sources[file_type].append(fp)
                
                # 根据文件类型选择统计模式
                # if file_type == "report":
                #     total_rows += sum(
                #         self.sheet_maps[fp][sheet].max_row for sheet in self.REPORT_SHEETS
                #     )
                # else:
                total_rows += self.sheet_maps[fp][sheet_name].max_row

            # 初始化进度条（总行数为所有非snt文件行数之和）
            progress.init_main_progress(desc="正在合并数据", total=total_rows)

            # 处理每个文件类型的数据
            for folder_type, file_paths in folder_sources.items():
                Logger.info(f"🔄 正在处理 [{folder_type}] 文件夹内数据...")
                
                # 文件类型决定采用的映射配置文件
                column_mapping =  self.bc4_report_mapping if "report" in folder_type.lower() else  self.response_mapping
                if not column_mapping:
                    Logger.info(f"⚠️ 未找到 [{folder_type}] 的列映射配置")

                # 遍历当前文件类型（文件夹下）的所有文件的sheet_name表
                for fp in file_paths:
                    file_sheets = self.sheet_maps[fp]
                    # 智能获取有效工作表
                    input_ws = self._get_valid_sheet(file_sheets, sheet_name)
                    
                    if not input_ws:
                        Logger.error(f"🛑 文件 {Path(fp).name} 无有效工作表")
                        continue
                    
                    # 获取当前有效工作表的行生成器，检查REQUIRED_FIELDS是否存在数据，都不存在会报错
                    data_gen = ExcelProcessor.excel_row_generator(
                        input_ws,
                        fp,
                        progress,
                        self.REQUIRED_FIELDS,
                        strict_flag=False
                    )
                    roll_back = True
                    # 逐行处理response_sheet_name，找对应的snt数据匹配更新后添加到文件中
                    for row in data_gen:
                        roll_back = False
                        key = tuple(row[field] for field in self.KEY_FIELDS)
                        if key not in snt_data:
                            Logger.debug(f"未找到匹配项: {key}，跳过更新")
                            continue
                        
                        # # 生成一行空数据（列名: None）
                        # base_row = {header: '' for header in headers}
                        # # 执行列映射更新
                        # base_row.update(ExcelProcessor.column_mapping(snt_data[key], self.snt_mapping))
                        # # base_row.update(snt_data[key])
                        base_data[key].update(ExcelProcessor.column_mapping(row, column_mapping))

                        # new_rows.append(base_row)
                        # Logger.info(f"更新 {key} 的 {column_mapping} 列")
                        # for src_col, dest_col in column_mapping.items():
                        #     if src_col in row:
                        #         base_row[dest_col] = row[src_col]
                        #         Logger.debug(f"更新 {key} 的 {dest_col} 列")
                    if roll_back:
                        # 回退检查默认表
                        for default_sheet_name in self.DEFAULT_FALLBACK_SHEETS:
                            ws = file_sheets.get(default_sheet_name)
                            if ws and self._validate_sheet_headers(ws):
                                Logger.info(f"文件{fp}⏩ 使用回退表 [{default_sheet_name}]")
                                data_gen = ExcelProcessor.excel_row_generator(
                                    ws,
                                    fp,
                                    progress,
                                    self.REQUIRED_FIELDS,
                                    strict_flag=False
                                )
                                for row in data_gen:
                                    key = tuple(row[field] for field in self.KEY_FIELDS)
                                    if key not in snt_data:
                                        Logger.debug(f"未找到匹配项: {key}，跳过更新")
                                        continue
                                    
                                    base_data[key].update(ExcelProcessor.column_mapping(row, column_mapping))

                            
            # ----------------------------
            # 阶段三：写入最终数据
            # ----------------------------
            
            # # 清空原有数据（如果存在）
            # output_ws.delete_rows(1, output_ws.max_row)
            
            # # 写入标题行
            # output_ws.append(self.REQUIRED_FIELDS)
            ordered_rows = ExcelProcessor.sort_generated_rows(base_data.values(), output_ws)
            for row in ordered_rows:
                output_ws.append(row)
            # # 写入数据行
            # for row in processed_rows:
            #     ordered_row = [row.get(field, "") for field in self.REQUIRED_FIELDS]
            #     output_ws.append(ordered_row)

            progress.close()
            Logger.info(f"✅ 工作表 [{sheet_name}] 处理完成，共更新 {len(base_data.values())} 行数据")
            return True

        except Exception as e:
            Logger.error(f"❌ 工作表 [{sheet_name}] 处理失败: {str(e)}")
            progress.close() if 'progress' in locals() else None
            return False
    def _get_valid_sheet(self, file_sheets, sheet_name):
        """动态获取有效工作表"""
        # 首选目标目标表名
        if sheet_name in file_sheets:
            return file_sheets[sheet_name]
        
        # 回退检查默认表
        for sheet_name in self.DEFAULT_FALLBACK_SHEETS:
            ws = file_sheets.get(sheet_name)
            if ws and self._validate_sheet_headers(ws):
                Logger.info(f"⏩ 使用回退表 [{sheet_name}]")
                return ws
        
        return None

    def _validate_sheet_headers(self, worksheet):
        """验证工作表表头是否包含关键字段"""
        try:
            # 读取首行作为表头
            header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
            return all(field in header_row for field in self.KEY_FIELDS)
        except Exception as e:
            Logger.error(f"表头验证失败: {str(e)}")
            return False
    def _get_folder_type(self, file_path):
        """从文件路径解析文件夹类型"""
        # 示例逻辑：假设路径结构为 /root/[folder_type]/filename.xlsx
        # if os.path.commonpath([file_path, self.report_path]) == self.report_path:
        #     return "report"
        if os.path.commonpath([file_path, self.response_path]) == self.response_path:
            return "response"
        else:
            return "other"
    # def _process_single_sheet(self, sheet_name, output_wb):
    #     """处理单个工作表"""
    #     try:
    #         Logger.info(f"🔨 开始处理工作表 [{sheet_name}]")
    #         output_ws = output_wb[sheet_name]
            
    #         # 初始化进度条
    #         progress = ProgressManager()
    #         max_rows = max(
    #             self.sheet_maps[file_path][sheet_name].max_row
    #             for file_path in self.sheet_maps.keys()
    #         )

    #         progress.init_main_progress(desc="已处理数据行数" , total=max_rows)

    #         # 遍历所有excel的sheet_name表，将其生成器集合在merge_data中
    #         merged_data = []
    #         for file_absolute_path in self.sheet_maps.keys():
    #             input_ws = self.sheet_maps[file_absolute_path][sheet_name]
    #             data_gen = ExcelProcessor.excel_row_generator(
    #                 input_ws,
    #                 file_absolute_path,
    #                 progress,
    #                 self.REQUIRED_FIELDS
    #             )
    #             merged_data.extend(data_gen)

    #         # 根据当前所有sheet_name的生成器，并返回处理后的结果
    #         processed_rows = self._process_data(merged_data, output_ws, progress)
    #         list(map(lambda row: output_ws.append(row), processed_rows))
            
    #         progress.close()
    #         Logger.info(f"✅ 工作表 [{sheet_name}] 处理完成，生成 {len(processed_rows)} 行数据")
    #         return True
            
    #     except Exception as e:
    #         Logger.error(f"❌ 工作表 [{sheet_name}] 处理失败: {str(e)}")
    #         return False

    def _process_data(self, data_generator, output_ws, progress):
        """数据处理核心逻辑"""
        processed_rows = []
        # data_generator是report和response文件当前sheet_name表的生成器，包含当前sheet_name表的所有有效行数据
        # 
        try:
            with GlobalThreadPool.get_executor() as executor:
                futures = []
                for row in data_generator:
                    future = executor.submit(
                        self._process_single_row,
                        row_data=row,
                        output_sheet=output_ws
                    )
                    futures.append(future)
                    progress.update()

                # 收集处理结果
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result(timeout=30)
                        if result:
                            processed_rows.extend(result)
                    except Exception as e:
                        Logger.error(f"数据处理异常: {str(e)}")
        except Exception as e:
            Logger.error(f"数据处理失败: {str(e)}")
            raise
        
        return processed_rows

    def _process_single_row(self, row_data, output_sheet):
        """单行数据处理逻辑"""
        try:
            # 执行字段映射
            mapped_row = {}
            mapped_row.update(ExcelProcessor.fixed_mapping(self.fixed_mapping))
            mapped_row.update(ExcelProcessor.column_mapping(row_data, self.snt_mapping))
            mapped_row.update(ExcelProcessor.column_mapping(row_data, self.bc4_report_mapping))
            
            # 按模板顺序排序
            return ExcelProcessor.sort_generated_rows([mapped_row], output_sheet)
        except Exception as e:
            Logger.error(f"行数据处理失败: {str(e)}\n原始数据: {row_data}")
            return []

    def run(self):
        """主执行流程"""
        try:
            # 阶段1：初始化配置
            self._load_mappings()
            self._validate_input_files()

            # 阶段2：准备输出文件——给用户反馈的snt文件
            absolute_path = FileParser.create_newfile_by_template(
                self.template_file,
                self.target_file,
                additional_columns=["列1", "列2"]
            )
            output_wb = load_workbook(absolute_path)
            # # 确保包含所有预置表
            # for sheet in self.sheet_names:
            #     if sheet not in output_wb.sheetnames:
            #         output_wb.create_sheet(sheet)

            # 阶段3：多表处理
            success_flags = []
            for sheet_name in self.sheet_names:
                success_flags.append(
                    self._process_single_sheet(sheet_name, output_wb)
                )

            # 阶段4：保存结果
            if all(success_flags):
                output_wb.save(self.target_file)
                Logger.info(f"💾 结果文件保存成功: {self.target_file}")
                return True
            else:
                raise RuntimeError("部分工作表处理失败")
                
        except Exception as e:
            Logger.error(f"❌ 主流程执行失败: {str(e)}")
            if os.path.exists(self.target_file):
                os.remove(self.target_file)
                Logger.error("已删除不完整的结果文件")
            return False
        finally:
            GlobalThreadPool.shutdown()

if __name__ == "__main__":
    processor = AutoSntProcessor()
    if processor.run():
        Logger.info("🎉 自动化处理完成！")
    else:
        Logger.error("❌ 处理过程中存在错误")
    input("按回车键退出...")