# 佇列批次消耗設計
嘗試設計累積工作到佇列，由工作者批次消耗。
當佇列中無工作時，工作者將等待下個工作進入佇列，或是隔一段時間後處理手中的批次。