# Databricks notebook source
class salesInvoiceStreamProcessing():
    def __init__(self):
        self.base_data_dir = "/FileStore/explore_projects/"

    def invoiceSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
            """

    def createReadDataframe(self):
        return (spark.readStream
                    .format("json")
                    .schema(self.invoiceSchema())
                    .load(f"{self.base_data_dir}/land/data/invoices/")
                )   

    def explodeSalesInvoices(self, invoiceDF):
        return ( invoiceDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                      "DeliveryAddress.State","DeliveryAddress.PinCode", 
                                      "explode(InvoiceLineItems) as LineItem")
                                    )  
           
    def flattenSalesInvoices(self, explodedDF):
        from pyspark.sql.functions import expr
        return( explodedDF.withColumn("ItemCode", expr("LineItem.ItemCode"))
                        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                        .withColumn("ItemQty", expr("LineItem.ItemQty"))
                        .withColumn("TotalValue", expr("LineItem.TotalValue"))
                        .drop("LineItem")
                )
        
    def appendInvoicesToTarget(self, flattenedDF):
        return (flattenedDF.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoices")
                    .outputMode("append")
                    .toTable("invoice_line_items")
        )

    def process(self):
           print(f"Starting Invoice Processing Stream...", end='')
           invoicesDF = self.createReadDataframe()
           explodedDF = self.explodeSalesInvoices(invoicesDF)
           resultDF = self.flattenSalesInvoices(explodedDF)
           sQuery = self.appendInvoicesToTarget(resultDF)
           print("Done\n")
           return sQuery  

# COMMAND ----------

startStream = salesInvoiceStreamProcessing()
streamQuery = startStream.process()


# COMMAND ----------

streamQuery.stop()
