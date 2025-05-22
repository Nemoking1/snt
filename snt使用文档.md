# SNT脚本使用手册

  本脚本适用于管理人员KA，进行信息管理及核对

## 一. 配置文件

1. template文件——决定输出文件的格式
2. sheet_config.txt——关键表单名（可多选）

---

## 二.基本操作

1. 在脚本启动路径下，安置template.xlsx文件，作为模板文件，仅包含一行数据（表头）作为生成的标准格式。
2. 在snt文件夹下导入一个或多个snt文件，文件名不固定。
   1. snt文件是客户发送的excel文件，里面有多个sheet，其中主要关注CREATED,COORDINATED,REQUESTED,BOOKED四个Sheet，每个sheet里面均存在数据。
   2. 脚本将检查所有snt文件的主要的sheet是否存在且有数据，如若不然，提示异常，并终止程序。
   3. 主要关注的sheet表单可通过配置文件夹下的sheet_config文件进行配置，如：/conf/sheet_config.txt，内容为CREATED,COORDINATED,REQUESTED,BOOKED。
3. 在response文件夹下导入多个excel，文件名称不固定，
   1. 脚本将检查每个文件中主要的四个sheet是否存在且有数据，如若不然，将检查是否存在其他表单，如果有，则读取该表单信息，反之提示异常，并终止程序。
4. 运行脚本
   1. 遍历所有snt文件
      1. 复制template表头到新生成的文件中/target/processed_snt_【时间戳】.xlsx
      2. 处理当前snt文件所有行（除去第一行表头），按照每行的表头信息和target文件进行比对赋值。
      3. 