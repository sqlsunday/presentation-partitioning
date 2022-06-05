USE [Partitioning Zero to Hero];
GO





SET STATISTICS TIME, IO ON;

DELETE FROM dbo.AccountTransactions_unpartitioned
WHERE YEAR(TransactionDate)=2020;





--- Look at the partitioned table:
EXECUTE dbo.sp_show_partitions
    @partition_scheme_name='Annual',
    @table='dbo.AccountTransactions';

BEGIN TRANSACTION;

    --- Truncate the 2020 partition:
    TRUNCATE TABLE dbo.AccountTransactions WITH (PARTITIONS (4));

ROLLBACK TRANSACTION;


USE master;
