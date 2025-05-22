from datetime import datetime
import os
import concurrent.futures
from pathlib import Path
from collections import defaultdict
from openpyxl import load_workbook
from sinotrans.core import FileParser, ExcelProcessor
from sinotrans.utils import Logger, GlobalThreadPool, ProgressManager
import warnings
import traceback

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

class AutoSntProcessor:
    # 预定义配置
    # 如果需要的表不存在，默认回退到默认表(可多个)
    DEFAULT_SHEET = "default_sheet"
    # 需要的表
    REQUIRED_SHEET = "required_sheet"
    # 用于关联不同文件中的行数据，表中必须存在的字段
    KEY_FIELDS = "key_fields"
    # 用于检验表中数据的有效性，通常和strice_flag配合使用
    REQUIRED_FIELDS = "required_fields"

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
        self.response_path = os.path.join(self.current_dir, "res")
        
        self.template_file = os.path.join(self.current_dir, "template.xlsx")
        self.target_file = os.path.join(self.target_path, f"output_{self.timestamp}.xlsx")

        self.sheet_config_file = os.path.join(self.config_path, "sheet_config.txt")
        self.fixed_mapping_file = os.path.join(self.config_path, "fixed_mapping.txt")
        self.pending_po_mapping_file = os.path.join(self.config_path, "pending_po_mapping.txt")
        self.response_mapping_file = os.path.join(self.config_path, "response_mapping.txt")
        self.target_file = os.path.join(self.target_path, f"output_{self.timestamp}.xlsx")
        
        FileParser.ensure_directories_exist([
            self.target_path, self.config_path,
            self.snt_path, self.response_path
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
            sheet_conf = FileParser.parse_mapping_dict(self.sheet_config_file,':', '|', ',', '=')
            self.default_fallback_sheets = sheet_conf.get(self.DEFAULT_SHEET).field_name.split(",")
            self.key_fields = sheet_conf.get(self.KEY_FIELDS).field_name.split(",")
            self.required_fields = sheet_conf.get(self.REQUIRED_FIELDS).field_name.split(",")
            self.sheet_names = sheet_conf.get(self.REQUIRED_SHEET).field_name.split(",")

            # self.sheet_names = FileParser.parse_conf(self.sheet_config_file, ',')
            self.fixed_mapping = FileParser.parse_mapping_dict(self.fixed_mapping_file,':', '|', ',', '=')   # 模板值映射
            self.snt_mapping = FileParser.parse_mapping_dict_of_list(self.pending_po_mapping_file,':', '|', ',', '=')
            self.response_mapping = FileParser.parse_mapping_dict_of_list(self.response_mapping_file,':', '|', ',', '=')
    
            Logger.info("✅ 映射文件加载成功")
        except Exception as e:
            Logger.error(f"❌ 映射文件加载失败: {str(e)}")
            raise

    def _validate_input_files(self):
        """验证输入文件完整性"""
        try:
            self.snt_files = FileParser.read_files(self.snt_path, [".xlsx", ".xls"])
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
            Logger.info("✅ 文件验证通过")
        except Exception as e:
            Logger.error(f"❌ 文件验证失败: {str(e)}")
            raise

    def _get_valid_sheet(self, file_sheets, sheet_name):
        """
        动态获取有效工作表
        参数：
        file_sheets: 工作表字典
        sheet_name: 目标工作表名
        返回值：
        - 工作表对象
        - 是否有使用回退表
        """
        # 首选目标目标表名
        if sheet_name in file_sheets:
            return file_sheets[sheet_name], True
        
        # 回退检查默认表
        for sheet_name in self.default_fallback_sheets:
            ws = file_sheets.get(sheet_name)
            if ws and self._validate_sheet_headers(ws):
                Logger.info(f"⏩ 使用回退表 [{sheet_name}]")
                return ws, False
        
        return None, False

    def _validate_sheet_headers(self, worksheet):
        """验证工作表表头是否包含关键字段"""
        try:
            # 读取首行作为表头
            header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
            return all(field in header_row for field in self.key_fields)
        except Exception as e:
            Logger.error(f"表头验证失败: {str(e)}")
            return False
    def _get_folder_type(self, file_path):
        """从文件路径解析文件夹类型"""
        if os.path.commonpath([file_path, self.response_path]) == self.response_path:
            return os.path.basename(self.response_path)
        if os.path.commonpath([file_path, self.snt_path]) == self.snt_path:
            return os.path.basename(self.snt_path)
        else:
            return "other"

    def _process_single_row(self, input_ws, fp, progress, snt_data, base_data, column_mapping):

        # 获取当前有效工作表的行生成器，检查REQUIRED_FIELDS是否存在数据，都不存在会报错
        data_gen = ExcelProcessor.excel_row_generator(
            input_ws,
            fp,
            progress,
            self.required_fields,
            strict_flag=False
        )
        # 如果一条数据也遍历不到，则当前的工作表无效——不存在任何REQUIRED_FIELDS有值的情况，回滚到默认表
        has_valid_data = False
        for row in data_gen:
            has_valid_data = True
            key = tuple(row[field] for field in self.key_fields)
            if key not in snt_data:
                Logger.debug(f"未找到匹配项: {key}，跳过更新")
                continue
            
            base_data[key].update(ExcelProcessor.column_mapping(row, column_mapping))
            # Logger.info(f"更新 {key} 的 {column_mapping} 列")
        return has_valid_data

    def _process_single_sheet(self, sheet_name, output_wb):
        """处理单个工作表"""
        try:
            # 获取当前sheet_name工作表的输出句柄、表头列表，用于后续处理
            output_ws = output_wb[sheet_name]
            headers = [cell.value for cell in output_ws[1]]

            Logger.info(f"🔨 开始处理工作表 [{sheet_name}]")
            progress = ProgressManager()
            # ----------------------------
            # 阶段一：加载SNT基准数据到内存
            # ----------------------------
            snt_file = next((fp for fp in self.sheet_maps.keys() if self._get_folder_type(fp) == os.path.basename(self.snt_path)), None)
            if not snt_file:
                raise RuntimeError(f"未找到{self.snt_path}文件夹下的基准文件")

            snt_data = {}
            snt_ws = self.sheet_maps[snt_file][sheet_name]
            # 将snt当前sheet_name数据存在关键字段keys——用于联系数据，的行写入内存{key_tuple,row}
            snt_gen = ExcelProcessor.excel_row_generator(
                snt_ws,
                snt_file,
                progress,
                self.key_fields,
                strict_flag=False
            )
            for row in snt_gen:
                key = tuple(row[field] for field in self.key_fields)
                if key in snt_data:
                    Logger.info(f"⚠️ 发现重复基准数据: {key}")
                snt_data[key] = row

            progress.close()
            Logger.info(f"📥 已加载 {len(snt_data)} 条有效基准数据")

            # --------------------------------------------
            # 阶段二：创建含有key索引的新行，将snt数据映射进去
            # --------------------------------------------
            base_data = {}
            for key, snt_row in snt_data.items():
                # 获取目标列格式——也就是模板列格式
                base_row = {header: '' for header in headers}
                base_row.update(ExcelProcessor.fixed_mapping(self.fixed_mapping))
                base_row.update(ExcelProcessor.column_mapping(snt_row, self.snt_mapping))
                base_data[key] = base_row


            # -----------------------------------------------------------------
            # 阶段三：处理其他文件数据，以文件夹为读取单位，以sheet_name为写入单位
            # -----------------------------------------------------------------

            folder_sources = defaultdict(list)
            # total_rows = 0
            # 将sheet_maps中的fp按文件夹分类
            for fp in self.sheet_maps.keys():
                if fp == snt_file:
                    continue
                folder_sources[self._get_folder_type(fp)].append(fp)
                #total_rows += self.sheet_maps[fp][sheet_name].max_row

            # progress.init_main_progress(desc="正在合并数据", total=total_rows)

            # 文件夹
            for folder, fps in folder_sources.items():
                Logger.info(f"🔄 正在处理 [{folder}] 文件夹内数据...")
                
                column_mapping =  self.response_mapping
                if not column_mapping:
                    raise RuntimeError (f"⚠️ 未找到 [{folder}] 的列映射配置")

                # 当前文件夹的所有文件
                for fp in fps:
                    sheets_wb = self.sheet_maps[fp]
                    # 获取有效工作表(如果找不到Sheet_name，则使用默认回退表)
                    input_ws, is_defalut_sheet = self._get_valid_sheet(sheets_wb, sheet_name)
                    if not input_ws:
                        Logger.error(f"🛑 文件 {Path(fp).name} 无有效工作表")
                        continue
                    progress = ProgressManager()
                    roll_back = not self._process_single_row(input_ws, fp, progress, snt_data, base_data, column_mapping)
                    progress.close()
                    # 若表中无数据，且使用的不是默认表，则尝试获取默认表数据
                    if not is_defalut_sheet and roll_back:
                        has_valid_data = False
                        # 获取默认表
                        for default_sheet_name in self.default_fallback_sheets:
                            input_ws = self._get_valid_sheet(sheets_wb, default_sheet_name)
                            # 默认表有效
                            if input_ws and self._validate_sheet_headers(input_ws):
                                Logger.info(f"🛑 文件{fp}⏩ 使用回退表 [{default_sheet_name}]")
                                progress = ProgressManager()
                                # 不短路
                                has_valid_data = has_valid_data | self._process_single_row(input_ws, fp, progress, snt_data, base_data, column_mapping)
                                progress.close()
                        if not has_valid_data:
                            # 存在业务场景，sheet_name就是没有业务数据，也不存在默认表
                            Logger.info(f"⚠️ 文件{fp}:【{sheet_name}】中无有效数据")
                    
            # ----------------------------
            # 阶段三：写入最终数据
            # ----------------------------
            # 排序按表头排序
            ordered_rows = ExcelProcessor.sort_generated_rows(base_data.values(), output_ws)
            list(map(lambda row: output_ws.append(row), ordered_rows))
            Logger.info(f"✅ 工作表 [{sheet_name}] 处理完成，共更新 {len(base_data.values())} 行数据")
            return True

        except Exception as e:
            Logger.error(f"❌ 工作表 [{sheet_name}] 处理失败: {str(e)}")
            progress.close() if 'progress' in locals() else None
            return False
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
                # 直接改模板文件就行
                # additional_columns=["列1", "列2"] 
            )
            output_wb = load_workbook(absolute_path)

            # 阶段3：多表处理
            success_flags = []
            for sheet_name in self.sheet_names:
                success_flags.append(
                    self._process_single_sheet(sheet_name, output_wb)
                )

            # 阶段4：保存结果，但凡有一个sheet处理失败，则删除不完整的输出文件
            if all(success_flags):
                output_wb.save(self.target_file)
                Logger.info(f"💾 结果文件保存成功: {self.target_file}")
                return True
            else:
                raise RuntimeError("部分工作表处理失败")
                
        except Exception as e:
            Logger.error(f"❌ 主流程执行失败: {str(e)}")
            Logger.debug(f"{traceback.format_exc()}")
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