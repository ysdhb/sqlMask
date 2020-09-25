# 项目简介
数据脱敏是一种常见的保护数据安全的方法

- 动态脱敏

可以通过灵活配置脱敏策略，在使用数据的同时又不泄露数据

比如身份证号，获取地区信息可以将后面几位加密处理，获取出生年月可以加密其他位。

- 静态脱敏

导出数据时，离线使用，要将数据脱敏后才能导出。

- 继承脱敏

创建的临时表也需要继承原始表的脱敏策略

# 功能简介

## 前提

1.项目只考虑SQL语句形式的脱敏，并且是UDF形式的脱敏

2.不提供UDF的实现，通过重写SQL语句，查询还是在数据仓库

![项目架构图](https://github.com/xylou/sqlMask/blob/master/doc/%E9%A1%B9%E7%9B%AE%E6%9E%B6%E6%9E%84%E5%9B%BE.png)

基于 `Calcite` 的 `SqlValidatorImpl` 实现的查找原始列

利用 Calcite 在 SQL语句校验阶段，记录Column的上一个Select信息，递归查找

# 使用简介

### 暂时提供了两张表

#### 数据库sales有两张表

- DEPTS表
```
DEPTNO:int,NAME:string
```
- EMPS表
```
EMPNO:int,NAME:string,DEPTNO:int,
GENDER:string,CITY:string,EMPID:int,
AGE:int,SLACKER:boolean,MANAGER:boolean,JOINEDAT:date
```

- 脱敏策略配置了三个
```
sales.emps.name=hash_fun(1,5,col,'*')
sales.emps.deptno=hash_fun2(1,6,col,'*')
sales.depts.name=hash_fun3(1,7,col,'*')
```

`mvn clean install`打包并测试

通过 `idea` 
打开并启动`yhh.com.mask.MaskApplication`

对外rest接口，通过Postman打开发送`Post`请求


![postman 请求](https://github.com/xylou/sqlMask/blob/master/doc/postman%20request.jpg)

`http://127.0.0.1:8080/query`
请求体为：
```
{
    "sql":"select name from emps"
}
```

结果：
```
select hash_fun(1, 5, emps.name, '*') as name
from sales.emps as emps
```

`yhh.com.mask.sql.SqlMaskTest` 提供了必要的测试用例查看

