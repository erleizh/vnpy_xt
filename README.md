# VeighNa框架的迅投研数据服务接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.4.6-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于迅投XtQuant封装开发的实时行情和数据服务接口，支持以下中国金融市场的K线和Tick数据：

* 股票、基金、债券、ETF期权：
  * SSE：上海证券交易所
  * SZSE：深圳证券交易所
* 期货、期货期权：
  * CFFEX：中国金融期货交易所
  * SHFE：上海期货交易所
  * DCE：大连商品交易所
  * CZCE：郑州商品交易所
  * INE：上海国际能源交易中心
  * GFEX：广州期货交易所


## 安装

安装环境推荐基于4.0.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com/)】。

下载解压后在cmd中运行：

```
pip install .
```


**客户端连接**

1. 连接请先登录券商版miniQMT，同时确保xtquant模块可以正常加载（点击【下载Python库】-【Python库下载】，下载完成后拷贝“Python库路径”下Lib\site-packages文件夹中的xtquant包到自己使用的Python环境的site_packages文件夹下）。
2. 在Veighna Trader的【全局配置】处进行数据服务配置：
    * datafeed.name：xt
    * datafeed.username：client
    * datafeed.password：留空
3. 请注意以客户端方式连接时，需要保持miniQMT客户端的运行。

**版本兼容**

1. 支持国金证券QMT（2024-10-16）版本
