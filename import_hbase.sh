#!/usr/bin/env bash

echo "This script is deprecated"

#for d in 20180104 20180105 20180108 20180109 20180110; do
#    hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
#    -Dimporttsv.separator=,  \
#    -Dimporttsv.columns="HBASE_ROW_KEY,data:uiDateTime,data:uiPreClosePx,data:uiOpenPx,data:uiHighPx,data:uiLowPx,data:uiLastPx,data:uiNumTrades,data:strInstrumentStatus,data:uiTotalVolumeTrade,data:uiTotalValueTrade,data:uiTotalBidQty,data:uiTotalOfferQty,data:uiWeightedAvgBidPx,data:uiWeightedAvgOfferPx,data:uiWithdrawBuyNumber,data:uiWithdrawSellNumber,data:uiWithdrawBuyAmount,data:uiWithdrawBuyMoney,data:uiWithdrawSellAmount,data:uiWithdrawSellMoney,data:uiTotalBidNumber,data:uiTotalOfferNumber,data:uiBidTradeMaxDuration,data:uiOfferTradeMaxDuration,data:uiNumBidOrders,data:uiNumOfferOrders,data:arrBidPrice,data:arrBidOrderQty,data:arrBidNumOrders,data:arrBidOrders,data:arrOfferPrice,data:arrOfferOrderQty,data:arrOfferNumOrders,data:arrOfferOrders,data:uiIOPV,data:uiETFBuyNumber,data:uiETFBuyAmount,data:uiETFBuyMoney,data:uiETFSellNumber,data:uiETFSellAmount,data:uiETFSellMoney" \
#    hisdata30g \
#    hdfs://srv1:9000/30Gdata/${d}/data.csv
#done


