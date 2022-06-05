USE master;
GO
IF (DB_ID('Partitioning Zero to Hero_source') IS NOT NULL)
    DROP DATABASE [Partitioning Zero to Hero_source];
GO
CREATE DATABASE [Partitioning Zero to Hero_source]
    ON (
        NAME = N'Partitioning Zero to Hero_source',
        FILENAME = N'D:\Stuff\Partitioning Zero to Hero_source.mdf',
        SIZE=256MB, FILEGROWTH=256MB)
    LOG ON (
        NAME = N'Partitioning Zero to Hero_source_log',
        FILENAME = N'D:\Stuff\Partitioning Zero to Hero_source_log.ldf',
        SIZE=256MB, FILEGROWTH=256MB)
;
GO
USE [Partitioning Zero to Hero_source];
GO
ALTER DATABASE CURRENT SET RECOVERY SIMPLE WITH NO_WAIT
GO

CREATE PARTITION FUNCTION [AnnualFunction](date)
AS RANGE RIGHT
FOR VALUES ('2018-01-01', '2019-01-01', '2020-01-01', '2021-01-01', '2022-01-01', '2023-01-01');

CREATE PARTITION SCHEME [Annual]
AS PARTITION [AnnualFunction]
ALL TO ([PRIMARY]);




CREATE TABLE dbo.AccountTransactions (
    TransactionDate         date NOT NULL,
    AccountID               bigint NOT NULL,
    TransactionID           bigint NOT NULL,
    Amount                  numeric(18, 2) NOT NULL
);

CREATE UNIQUE CLUSTERED INDEX PK_AccountTransactions
    ON dbo.AccountTransactions (AccountID, TransactionDate, TransactionID)
    WITH (DATA_COMPRESSION=PAGE) ON Annual(TransactionDate);

INSERT INTO dbo.AccountTransactions WITH (TABLOCKX, HOLDLOCK) (TransactionDate, AccountID, TransactionID, Amount)
SELECT TransactionDate,
       AccountID, 
       1000000+ROW_NUMBER() OVER (ORDER BY AccountID, TransactionDate) AS TransactionID,
       Amount
FROM (
    SELECT DATEADD(day, b.column_id*b.system_type_id, '2017-12-01') AS TransactionDate,
           2200000000+CAST(CHECKSUM(a.[object_id], a.[name], a.column_id) AS bigint) AS AccountID,
           ROUND(10000.*RAND(CHECKSUM(NEWID())), 2)-5000 AS Amount
    FROM sys.columns AS a
    CROSS JOIN sys.columns AS b
    INNER JOIN sys.columns AS c ON c.[object_id]=5
    ) AS x
WHERE TransactionDate BETWEEN '2018-01-01' AND SYSDATETIME();

GO
IF (DB_ID('Partitioning Zero to Hero') IS NOT NULL)
    DROP DATABASE [Partitioning Zero to Hero];
GO
CREATE DATABASE [Partitioning Zero to Hero]
    ON (
        NAME = N'Partitioning Zero to Hero',
        FILENAME = N'D:\Stuff\Partitioning Zero to Hero.mdf',
        SIZE=256MB, FILEGROWTH=256MB)
    LOG ON (
        NAME = N'Partitioning Zero to Hero_log',
        FILENAME = N'D:\Stuff\Partitioning Zero to Hero_log.ldf',
        SIZE=256MB, FILEGROWTH=256MB)
GO
USE [Partitioning Zero to Hero];
GO
--ALTER DATABASE CURRENT SET RECOVERY SIMPLE WITH NO_WAIT
GO

ALTER DATABASE CURRENT ADD FILEGROUP [Some_filegroup];

ALTER DATABASE CURRENT ADD FILE (
        NAME='Some_file.ndf',
        SIZE=256MB, FILEGROWTH=256MB,
        FILENAME='D:\Stuff\Some_file.ndf'
    ) TO FILEGROUP [Some_filegroup];

--DROP TABLE dbo.AccountTransactions_unpartitioned;

CREATE TABLE dbo.AccountTransactions_unpartitioned (
    TransactionDateYear AS YEAR(TransactionDate) PERSISTED NOT NULL,
    TransactionDate         date NOT NULL,
    AccountID               bigint NOT NULL,
    TransactionID           bigint NOT NULL,
    Amount                  numeric(18, 2) NOT NULL,
    Filler                  binary(250) NOT NULL DEFAULT (0x00),
    CONSTRAINT PK_AccountTransactions_unpartitioned PRIMARY KEY CLUSTERED (TransactionDateYear, AccountID, TransactionDate, TransactionID)
) ON Some_Filegroup;

INSERT INTO dbo.AccountTransactions_unpartitioned (TransactionDate, AccountID, TransactionID, Amount)
SELECT TransactionDate, AccountID, TransactionID, Amount
FROM [Partitioning Zero to Hero_source].dbo.AccountTransactions;




CREATE TABLE dbo.AccountTransactions_plain (
    TransactionDate         date NOT NULL,
    AccountID               bigint NOT NULL,
    TransactionID           bigint NOT NULL,
    Amount                  numeric(18, 2) NOT NULL,
    Filler                  binary(250) NOT NULL DEFAULT (0x00),
    CONSTRAINT PK_AccountTransactions_plain PRIMARY KEY CLUSTERED (AccountID, TransactionDate, TransactionID)
) ON Some_Filegroup;

INSERT INTO dbo.AccountTransactions_plain (TransactionDate, AccountID, TransactionID, Amount)
SELECT TransactionDate, AccountID, TransactionID, Amount
FROM [Partitioning Zero to Hero_source].dbo.AccountTransactions;




GO
CREATE OR ALTER PROCEDURE dbo.sp_show_partitions
    @partition_scheme_name  sysname,
    @table sysname
AS

    --- Collect partition function and the partition function's
    --- range boundary values
    WITH fn AS (
        SELECT ps.data_space_id,
               pf.function_id, pf.boundary_value_on_right,
               prv.boundary_id, prv.[value]
        FROM sys.partition_functions AS pf
        INNER JOIN sys.partition_range_values AS prv ON pf.function_id=prv.function_id
        INNER JOIN sys.partition_schemes AS ps ON ps.function_id=pf.function_id
        WHERE ps.[name]=@partition_scheme_name
        ),

    --- Compute the ranges
    ranges AS (
        SELECT data_space_id,
               boundary_id as partition_number,
               NULL AS lower_boundary,
               boundary_value_on_right,
               [value] AS upper_boundary
        FROM fn
        WHERE boundary_id=1
 
        UNION ALL
 
        SELECT data_space_id,
               boundary_id+1 as partition_number,
               [value] AS lower_boundary,
               boundary_value_on_right,
               LEAD([value], 1) OVER (ORDER BY boundary_id) AS upper_boundary
        FROM fn)

    --- Return the range that applies for @partition_number
    SELECT ISNULL(i.[type_desc], 'HEAP') AS [Type],
           i.[name] AS [Index name],
           ranges.partition_number AS [Partition],
           ranges.lower_boundary AS [Lower boundary],
           CAST((CASE WHEN ranges.lower_boundary IS NULL THEN '   '
                      WHEN ranges.boundary_value_on_right=0 THEN '<'
                      ELSE '<=' END) AS varchar(2))+'   '+c.[name]+N'   '+
           CAST((CASE WHEN ranges.upper_boundary IS NULL THEN ' '
                      WHEN ranges.boundary_value_on_right=1 THEN '<'
                      ELSE '<=' END) AS varchar(2)) AS [Range],
           ranges.upper_boundary AS [Upper boundary],
           fg.[name] AS [Filegroup],
           p.[rows] AS [Row count],
           p.data_compression_desc AS [Compression],
           (CASE WHEN fg.is_read_only=1 THEN 'READ_ONLY' ELSE '' END) AS [Read-only]
    FROM ranges
    LEFT JOIN sys.destination_data_spaces AS dds ON dds.partition_scheme_id=ranges.data_space_id AND dds.destination_id=ranges.partition_number
    LEFT JOIN sys.filegroups AS fg ON dds.data_space_id=fg.data_space_id
    LEFT JOIN sys.partitions AS p ON p.[object_id]=OBJECT_ID(@table) AND p.index_id IN (0, 1) AND p.partition_number=ranges.partition_number
    LEFT JOIN sys.indexes AS i ON p.[object_id]=i.[object_id] AND p.index_id=i.index_id
    LEFT JOIN sys.index_columns AS ic ON p.[object_id]=ic.[object_id] AND p.index_id=ic.index_id AND ic.partition_ordinal=1
    LEFT JOIN sys.columns AS c ON ic.[object_id]=c.[object_id] AND c.column_id=ic.column_id
    ORDER BY ranges.partition_number;

GO
CREATE OR ALTER PROCEDURE dbo.sp_truncate_partitions
    @data_space_id              int,
    @first_boundary             sql_variant,
    @last_boundary              sql_variant,
    @truncate_before_first_boundary bit=0,
    @truncate_after_last_boundary bit=0
AS

SET NOCOUNT ON;

DECLARE @sql nvarchar(max)=NULL;

--- Any partitions that we can truncate right away?
SELECT @sql=ISNULL(@sql+N'
', N'')+N'TRUNCATE TABLE '+QUOTENAME(s.[name])+N'.'+QUOTENAME(t.[name])+N' WITH (PARTITIONS ('+STRING_AGG(CAST(x.partition_number AS nvarchar(10)), N', ') WITHIN GROUP (ORDER BY x.partition_number)+N'));'
FROM sys.partition_functions AS pf
INNER JOIN sys.partition_range_values AS prv ON pf.function_id=prv.function_id
INNER JOIN sys.partition_schemes AS ps ON ps.function_id=pf.function_id
INNER JOIN sys.indexes AS i ON i.data_space_id=@data_space_id AND i.index_id IN (0, 1)
INNER JOIN sys.tables AS t ON i.[object_id]=t.[object_id]
INNER JOIN sys.schemas AS s ON t.[schema_id]=s.[schema_id]
INNER JOIN sys.partitions AS p ON p.[object_id]=t.[object_id] AND p.index_id=i.index_id AND p.partition_number=prv.boundary_id AND p.[rows]>0
CROSS APPLY (
    SELECT prv.boundary_id+1 AS partition_number
    WHERE @truncate_after_last_boundary=1 AND (
          pf.boundary_value_on_right=1 AND prv.[value]>@last_boundary
       OR pf.boundary_value_on_right=0 AND prv.[value]>=@last_boundary)

    UNION ALL

    SELECT prv.boundary_id AS partition_number
    WHERE @truncate_before_first_boundary=1 AND (
          pf.boundary_value_on_right=1 AND prv.[value]<=@first_boundary
       OR pf.boundary_value_on_right=0 AND prv.[value]<@first_boundary)
    ) AS x
WHERE ps.data_space_id=@data_space_id
GROUP BY s.[name], t.[name];

PRINT @sql;

IF (@sql IS NOT NULL)
    EXECUTE sys.sp_executesql @sql;

GO

CREATE OR ALTER PROCEDURE dbo.sp_partitions
    @pf_name                    sysname,
    @ps_name                    sysname,
    @boundary_on_right          bit=NULL,
    @first_boundary             sql_variant,
    @boundary_increment         sql_variant,
    @last_boundary              sql_variant,
    @filegroup_name_template    sysname=N'FG_*',
    @file_name_template         sysname=NULL,
    @file_path                  nvarchar(max)=NULL,
    @initial_file_size          varchar(100)=NULL,
    @file_auto_growth           varchar(100)=NULL,
    @truncate_before_first_boundary bit=0,
    @truncate_after_last_boundary bit=0,
    @drop_unused_filegroups     bit=0,
    @read_only_before           sql_variant=NULL,
    @read_only_after            sql_variant=NULL,
    @read_write_before          sql_variant=NULL,
    @read_write_after           sql_variant=NULL
AS




IF (@file_name_template IS NULL)
    SET @file_name_template=DB_NAME()+N'_'+@filegroup_name_template;

IF (@filegroup_name_template NOT LIKE N'%*%' OR @filegroup_name_template LIKE N'%*%*%')
    THROW 50001, '@filegroup_name_template must contain a single asterisk to denote the boundary value.', 1;

IF (@file_name_template NOT LIKE N'%*%' OR @file_name_template LIKE N'%*%*%')
    THROW 50001, '@file_name_template must contain a single asterisk to denote the boundary value.', 1;

IF (@first_boundary IS NULL)
    THROW 50001, '@first_boundary cannot be NULL.', 1;

IF (@boundary_increment IS NULL)
    THROW 50001, '@boundary_increment cannot be NULL.', 1;

IF (@last_boundary IS NULL)
    THROW 50001, '@last_boundary cannot be NULL.', 1;

IF (@ps_name IS NULL AND @pf_name IS NOT NULL)
    SET @ps_name=@pf_name;

IF (@ps_name IS NOT NULL AND @pf_name IS NULL)
    SET @pf_name=@ps_name;

--TODO: validate syntax of @initial_file_size, @file_auto_growth


DECLARE @function_id        int,        -- sys.partition_functions (function_id)
        @data_space_id      int,        -- sys.partition_schemes (data_space_id)
        @basetype           sysname,
        @type               sysname,
        @sql                nvarchar(max),
        @msg                nvarchar(max);

DECLARE @increment_basetype sysname,
        @increment_num      numeric(28, 8),
        @increment_year     int,
        @increment_month    int,
        @increment_day      int,
        @increment_hour     int,
        @increment_minute   int,
        @increment_second   int,
        @first_boundary_num numeric(28, 8),
        @first_boundary_date datetime2(7);

DECLARE @boundaries TABLE (
    boundary_ordinal        int NULL,
    boundary_variant        sql_variant NULL,
    boundary_nvarchar       nvarchar(100) NOT NULL,
    partition_number        int NULL,
    [filegroup]             sysname NULL,
    [file]                  sysname NULL,
    INDEX IX CLUSTERED (boundary_ordinal)
);

DECLARE @filegroup_list TABLE (
    filegroup_ordinal       int NOT NULL,
    [filegroup]             sysname NOT NULL,
    PRIMARY KEY CLUSTERED (filegroup_ordinal)
);

SET @basetype=CAST(SQL_VARIANT_PROPERTY(@first_boundary, 'BaseType') AS sysname);
SET @increment_basetype=CAST(SQL_VARIANT_PROPERTY(@boundary_increment, 'BaseType') AS sysname);

IF (@basetype!=CAST(SQL_VARIANT_PROPERTY(@last_boundary, 'BaseType') AS sysname))
    THROW 50001, '@first_boundary and @last_boundary must have the same base type.', 1;

IF (@last_boundary<@first_boundary)
    THROW 50001, '@first_boundary must be smaller than or equalt to @last_boundary.', 1;

IF (@basetype NOT LIKE '%int' AND @basetype NOT IN ('float', 'decimal', 'numeric', 'real') AND @basetype NOT LIKE '%date%' AND @basetype NOT LIKE '%time')
    THROW 50001, 'Cannot use this data type to automatically increment.', 1;

SELECT @function_id=function_id, @boundary_on_right=ISNULL(boundary_value_on_right, @boundary_on_right)
FROM sys.partition_functions
WHERE [name]=@pf_name;

IF (@function_id IS NULL AND @boundary_on_right IS NULL) SET @boundary_on_right=1;

SELECT @data_space_id=data_space_id
FROM sys.partition_schemes
WHERE [name]=@ps_name;

--- If function and scheme exist, but don't belong to each other:
IF (@function_id!=(SELECT function_id FROM sys.partition_schemes WHERE data_space_id=@data_space_id))
    THROW 50001, 'This partition scheme does not use this partition function.', 1;

--- Cannot change datatype or boundary range left/right:
IF (@boundary_on_right!=(SELECT boundary_value_on_right FROM sys.partition_functions WHERE function_id=@function_id))
    THROW 50001, 'Cannot change the side of the boundary value (RANGE LEFT/RANGE RIGHT).', 1;

--- Figure out the full data type:
SELECT @type=@basetype+(CASE
            WHEN @basetype LIKE N'n%char' OR @basetype LIKE N'n%binary' THEN N'('+ISNULL(CAST(NULLIF(x.[MaxLength], -1)/2 AS nvarchar(10)), N'max')+N')'
            WHEN @basetype LIKE N'%char' OR @basetype LIKE N'%binary' THEN N'('+ISNULL(CAST(NULLIF(x.[MaxLength], -1) AS nvarchar(10)), N'max')+N')'
            WHEN @basetype IN (N'datetime2', N'datetimeoffset', 'time') THEN N'('+CAST(x.[Scale] AS nvarchar(10))+N')'
            WHEN @basetype IN (N'decimal', N'numeric') THEN N'('+CAST(x.[Precision] AS nvarchar(10))+N', '+CAST(x.[Scale] AS nvarchar(10))+N')'
            ELSE N'' END)
FROM (
    VALUES (
        CAST(SQL_VARIANT_PROPERTY(@first_boundary, 'Precision') AS int),
        CAST(SQL_VARIANT_PROPERTY(@first_boundary, 'Scale') AS int),
        CAST(SQL_VARIANT_PROPERTY(@first_boundary, 'MaxLength') AS int)
    )) AS x([Precision], [Scale], [MaxLength]);


SELECT @increment_num=(CASE
            WHEN @increment_basetype LIKE '%int' OR
                 @increment_basetype IN ('float', 'decimal', 'numeric', 'real')
            THEN TRY_CAST(@boundary_increment AS numeric(28, 8))
            ELSE 0 END),
       @increment_year=(CASE
            WHEN @increment_basetype LIKE '%date%'
            THEN DATEDIFF(year, CAST(0 AS datetime), TRY_CAST(@boundary_increment AS date))
            ELSE 0 END),
       @increment_month=(CASE
            WHEN @increment_basetype LIKE '%date%'
            THEN DATEPART(month, TRY_CAST(@boundary_increment AS date))-1
            ELSE 0 END),
       @increment_day=(CASE
            WHEN @increment_basetype LIKE '%date%'
            THEN DATEPART(day, TRY_CAST(@boundary_increment AS date))-1
            ELSE 0 END),
       @increment_hour=(CASE
            WHEN @increment_basetype LIKE '%time%'
            THEN DATEPART(hour, TRY_CAST(@boundary_increment AS datetime2(0)))
            ELSE 0 END),
       @increment_minute=(CASE
            WHEN @increment_basetype LIKE '%time%'
            THEN DATEPART(minute, TRY_CAST(@boundary_increment AS datetime2(0)))
            ELSE 0 END),
       @increment_second=(CASE
            WHEN @increment_basetype LIKE '%time%'
            THEN DATEPART(second, TRY_CAST(@boundary_increment AS datetime2(0)))
            ELSE 0 END);




--- Anyway, so there I was, trying to out-smart the SQL Server optimizer...
IF (@basetype LIKE '%int' OR @basetype IN ('float', 'decimal', 'numeric', 'real'))
    SET @first_boundary_num=TRY_CAST(@first_boundary AS numeric(28, 8))

IF (@basetype LIKE '%date%' OR @basetype LIKE '%time')
    SET @first_boundary_date=TRY_CAST(@first_boundary AS datetime2(7));



WITH cte AS (
    --- Numeric start value:
    SELECT 1 AS boundary_ordinal,
           CAST(@first_boundary_num-@increment_num AS sql_variant) AS boundary,
           -1 AS edge_boundary
    WHERE @basetype LIKE '%int' OR @basetype IN ('float', 'decimal', 'numeric', 'real')

    UNION ALL

    --- Date/time start value:
    SELECT 1 AS boundary_ordinal,
           CAST(DATEADD(second, 0-@increment_second,
                        DATEADD(minute, 0-@increment_minute,
                            DATEADD(hour, 0-@increment_hour,
                                DATEADD(day, 0-@increment_day-@increment_num,
                                    DATEADD(month, 0-@increment_month,
                                        DATEADD(year, 0-@increment_year, @first_boundary_date)))))) AS sql_variant) AS boundary,
           -1 AS edge_boundary
    WHERE @basetype LIKE '%date%' OR @basetype LIKE '%time'

    UNION ALL

    --- Add one increment:
    SELECT boundary_ordinal+1,
           (CASE
                 --- Numeric types
                 WHEN @basetype LIKE '%int' OR
                      @basetype IN ('float', 'decimal', 'numeric', 'real')
                 THEN CAST(TRY_CAST(boundary AS numeric(28, 8))+@increment_num AS sql_variant)

                 --- Date/time types
                 WHEN @basetype LIKE '%date%' OR
                      @basetype LIKE '%time'
                 THEN CAST(DATEADD(second, @increment_second,
                        DATEADD(minute, @increment_minute,
                            DATEADD(hour, @increment_hour,
                                DATEADD(day, @increment_day+@increment_num,
                                    DATEADD(month, @increment_month,
                                        DATEADD(year, @increment_year, TRY_CAST(boundary AS datetime2(7)))))))) AS sql_variant)

            END) AS boundary,
           (CASE WHEN boundary>=@last_boundary THEN 1 ELSE 0 END)+edge_boundary
    FROM cte
    WHERE edge_boundary<=0 -- boundary<=@last_boundary
),

cte2 AS (
    SELECT boundary_ordinal, boundary, LEAD(boundary, 1) OVER (ORDER BY boundary_ordinal) AS lead_boundary
    FROM cte
    WHERE @boundary_on_right=0 AND edge_boundary<=0

    UNION ALL

    SELECT boundary_ordinal-1, boundary, LEAD(boundary, 1) OVER (ORDER BY boundary_ordinal) AS lead_boundary
    FROM cte
    WHERE @boundary_on_right=1 AND boundary_ordinal>=1 AND edge_boundary<=0
)

INSERT INTO @boundaries (boundary_ordinal, boundary_variant, boundary_nvarchar, [filegroup], [file])
SELECT boundary_ordinal AS boundary_ordinal,
       boundary AS boundary_variant,
       --- Boundary value as T-SQL code (nvarchar):
       (CASE WHEN @basetype LIKE N'n%char' THEN N'N'+QUOTENAME(CAST(boundary AS nvarchar(max)), N'''')
             WHEN @basetype LIKE N'%char' THEN QUOTENAME(CAST(boundary AS nvarchar(max)), N'''')
             WHEN @basetype LIKE N'%binary' THEN CONVERT(nvarchar(100), boundary, 1)
             WHEN @basetype LIKE N'%date%' THEN N''''+REPLACE(CONVERT(nvarchar(19), boundary, 121), N' 00:00:00', N'')+N''''
             WHEN @basetype=N'time' THEN REPLACE(N''''+CONVERT(nvarchar(12), boundary, 114)+N'''', N'.000', N'')
             ELSE CAST(boundary AS nvarchar(max)) END) AS boundary_varchar,
       --- Filegroup name:
       REPLACE(@filegroup_name_template, N'*', (CASE
             WHEN @boundary_on_right=1 AND LAG(0, 1, 1) OVER (ORDER BY boundary_ordinal)=1 THEN N'START'
             WHEN @boundary_on_right=0 AND LEAD(0, 1, 1) OVER (ORDER BY boundary_ordinal)=1 THEN N'END'
             WHEN @basetype LIKE N'%char' THEN CAST(boundary AS nvarchar(max))
             WHEN @basetype LIKE N'%binary' THEN CONVERT(nvarchar(100), boundary, 1)
             WHEN @basetype LIKE N'%date%' THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(19), boundary, 126), N'T00:00:00', N''), N'-', N''), N'.', N''), N':', N''), N'T', N'_')
             WHEN @basetype=N'time' THEN REPLACE(CONVERT(nvarchar(8), boundary, 114), N':', N'_')
             ELSE CAST(boundary AS nvarchar(max)) END)) AS [filegroup],
       --- File name:
       REPLACE(@file_name_template, N'*', (CASE
             WHEN @boundary_on_right=1 AND LAG(0, 1, 1) OVER (ORDER BY boundary_ordinal)=1 THEN N'START'
             WHEN @boundary_on_right=0 AND LEAD(0, 1, 1) OVER (ORDER BY boundary_ordinal)=1 THEN N'END'
             WHEN @basetype LIKE N'%char' THEN CAST(boundary AS nvarchar(max))
             WHEN @basetype LIKE N'%binary' THEN CONVERT(nvarchar(100), boundary, 1)
             WHEN @basetype LIKE N'%date%' THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(19), boundary, 126), N'T00:00:00', N''), N'-', N''), N'.', N''), N':', N''), N'T', N'_')
             WHEN @basetype=N'time' THEN REPLACE(CONVERT(nvarchar(8), boundary, 114), N':', N'_')
             ELSE CAST(boundary AS nvarchar(max)) END)) AS [file]
FROM cte2
--WHERE @boundary_on_right=0 AND boundary<=@last_boundary
OPTION (MAXRECURSION 15000);








--- Create filegroups that don't exist.
--------------------------------------------------

SELECT @sql=NULL;
SELECT @sql=ISNULL(@sql+N'
', N'')+N'ALTER DATABASE CURRENT ADD FILEGROUP '+QUOTENAME([filegroup])+N';
'
FROM (
    SELECT DISTINCT [filegroup]
    FROM @boundaries
    WHERE [filegroup] NOT IN (SELECT [name]
                              FROM sys.filegroups
                              WHERE [type]=N'FG')) AS fg;

IF (@sql IS NOT NULL) BEGIN;
    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;
END;



--- Create files that don't exist.
--------------------------------------------------

IF (@file_path IS NULL)
    SELECT TOP (1) @file_path=LEFT(physical_name, LEN(physical_name)-CHARINDEX(N'\', REVERSE(physical_name))+1)
    FROM sys.database_files
    WHERE [type_desc]=N'ROWS'
    ORDER BY [file_id] DESC;

IF (@file_path NOT LIKE '%\')
    SET @file_path=@file_path+N'\';

SELECT @sql=NULL;
SELECT @sql=ISNULL(@sql+N'
', N'')+N'ALTER DATABASE CURRENT ADD FILE (
        NAME='+QUOTENAME([file], N'''')+N','+ISNULL(N'
        SIZE='+@initial_file_size+N',', N'')+ISNULL(N'
        FILEGROWTH='+@file_auto_growth+N',', N'')+N'
        FILENAME='+QUOTENAME(@file_path+[file]+(CASE WHEN [file] NOT LIKE N'%._df' THEN N'.ndf' ELSE N'' END), N'''')+N'
    ) TO FILEGROUP '+QUOTENAME([filegroup])+N';
'
FROM (
    SELECT DISTINCT [filegroup], [file]
    FROM @boundaries
    WHERE [file] NOT IN (SELECT [name]
                         FROM sys.database_files
                         WHERE [type_desc]=N'ROWS')) AS fg;

IF (@sql IS NOT NULL) BEGIN;
    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;
END;



SET @msg=NULL;
SELECT TOP (1) @msg=N'Database file '+QUOTENAME(df.[name])+N' belongs to filegroup '+ds.[name]+N', but should belong to '+b.[filegroup]+N'.'
FROM sys.database_files AS df
INNER JOIN sys.data_spaces AS ds ON df.data_space_id=ds.data_space_id
INNER JOIN @boundaries AS b ON b.[file]=df.[name]
WHERE df.[type_desc]=N'ROWS'
  AND b.[filegroup]!=ds.[name];

IF (@msg IS NOT NULL)
    THROW 50001, @msg, 1;





--- Update @boundaries.partition_number:
WITH val AS (
    SELECT prv.boundary_id, prv.[value], fg.[name] AS [filegroup],
           --- Boundary value as T-SQL code (nvarchar):
           (CASE WHEN @basetype LIKE N'n%char' THEN N'N'+QUOTENAME(CAST(prv.[value] AS nvarchar(max)), N'''')
                 WHEN @basetype LIKE N'%char' THEN QUOTENAME(CAST(prv.[value] AS nvarchar(max)), N'''')
                 WHEN @basetype LIKE N'%binary' THEN CONVERT(nvarchar(100), prv.[value], 1)
                 WHEN @basetype LIKE N'%date%' THEN N''''+REPLACE(CONVERT(nvarchar(19), prv.[value], 121), N' 00:00:00', N'')+N''''
                 WHEN @basetype=N'time' THEN REPLACE(N''''+CONVERT(nvarchar(12), prv.[value], 114)+N'''', N'.000', N'')
                 ELSE CAST(prv.[value] AS nvarchar(max)) END) AS boundary_varchar
    FROM sys.partition_functions AS pf
    INNER JOIN sys.partition_range_values AS prv ON pf.function_id=prv.function_id
    INNER JOIN sys.partition_schemes AS ps ON ps.function_id=pf.function_id
    LEFT JOIN sys.destination_data_spaces AS dds ON dds.partition_scheme_id=ps.data_space_id AND dds.destination_id=prv.boundary_id
    LEFT JOIN sys.filegroups AS fg ON dds.data_space_id=fg.data_space_id
    WHERE ps.data_space_id=@data_space_id)
    
MERGE INTO @boundaries AS b
USING val ON EXISTS (
    SELECT b.boundary_variant
    INTERSECT 
    SELECT val.[value])

WHEN MATCHED THEN
    UPDATE
    SET b.partition_number=val.boundary_id

WHEN NOT MATCHED BY TARGET THEN
    INSERT (boundary_variant, boundary_nvarchar, partition_number, [filegroup])
    VALUES (val.[value], boundary_varchar, val.boundary_id, val.[filegroup]);





--- Add (split) partitions
--------------------------------------------------

IF (@function_id IS NOT NULL AND @data_space_id IS NOT NULL) BEGIN;

    --- Any partitions that we can truncate right away?
    IF (1 IN (@truncate_before_first_boundary, @truncate_after_last_boundary))
        EXECUTE dbo.sp_truncate_partitions
            @data_space_id=@data_space_id,
            @first_boundary=@first_boundary,
            @last_boundary=@last_boundary,
            @truncate_before_first_boundary=@truncate_before_first_boundary,
            @truncate_after_last_boundary=@truncate_after_last_boundary;

    SET @sql=NULL;

    SELECT @sql=ISNULL(@sql+N'
', N'')+N'ALTER PARTITION SCHEME '+QUOTENAME(@ps_name)+N' NEXT USED '+QUOTENAME(b.[filegroup])+N';
ALTER PARTITION FUNCTION '+QUOTENAME(@pf_name)+N'() SPLIT RANGE ('+b.boundary_nvarchar+N');'
    FROM (
        SELECT boundary_ordinal, boundary_nvarchar, [filegroup], partition_number, ROW_NUMBER() OVER (ORDER BY boundary_variant) AS ord
        FROM @boundaries
        ) AS b
    WHERE b.partition_number IS NULL AND (
          @boundary_on_right=1 AND b.ord>1
       OR @boundary_on_right=0 AND b.boundary_ordinal<(SELECT MAX(boundary_ordinal) FROM @boundaries))
    ORDER BY b.ord DESC;

    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;

END;




--- Create the function if it does not exist.
--------------------------------------------------

IF (@function_id IS NULL) BEGIN;
    SELECT @sql=N'CREATE PARTITION FUNCTION '+QUOTENAME(@pf_name)+N'('+@type+N')'+
            N' AS RANGE '+(CASE WHEN @boundary_on_right=1 THEN N'RIGHT' ELSE N'LEFT' END)+
            N' FOR VALUES ('+STRING_AGG(CAST(b.boundary_nvarchar AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY b.boundary_ordinal)+
            N');'
    FROM @boundaries AS b
    WHERE @boundary_on_right=1 AND b.boundary_ordinal>=1
       OR @boundary_on_right=0 AND b.boundary_ordinal<(SELECT MAX(boundary_ordinal) FROM @boundaries);

    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;

    SELECT @function_id=function_id
    FROM sys.partition_functions
    WHERE [name]=@pf_name;
END;




--- Create the scheme if it does not exist.
--------------------------------------------------

IF (@data_space_id IS NULL) BEGIN;
    SELECT @sql=N'CREATE PARTITION SCHEME '+QUOTENAME(@ps_name)+N' AS PARTITION '+
            QUOTENAME(@pf_name)+
            (CASE WHEN COUNT(*)=1 THEN N' ALL TO ('+QUOTENAME(MIN(b.[filegroup]))+N');'
                  ELSE N' TO ('+STRING_AGG(CAST(QUOTENAME(b.[filegroup]) AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY b.boundary_ordinal)+N');'
                  END)
    FROM @boundaries AS b
    WHERE @boundary_on_right=1
       OR @boundary_on_right=0 AND b.boundary_ordinal<=(SELECT MAX(boundary_ordinal) FROM @boundaries);

    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;

    SELECT @data_space_id=data_space_id
    FROM sys.partition_schemes
    WHERE [name]=@ps_name;
END;



--- Remove (merge) partitions
--------------------------------------------------

--- Update @boundaries.partition_number:
WITH val AS (
    SELECT prv.boundary_id, prv.[value], fg.[name] AS [filegroup],
           --- Boundary value as T-SQL code (nvarchar):
           (CASE WHEN @basetype LIKE N'n%char' THEN N'N'+QUOTENAME(CAST(prv.[value] AS nvarchar(max)), N'''')
                 WHEN @basetype LIKE N'%char' THEN QUOTENAME(CAST(prv.[value] AS nvarchar(max)), N'''')
                 WHEN @basetype LIKE N'%binary' THEN CONVERT(nvarchar(100), prv.[value], 1)
                 WHEN @basetype LIKE N'%date%' THEN N''''+REPLACE(CONVERT(nvarchar(19), prv.[value], 121), N' 00:00:00', N'')+N''''
                 WHEN @basetype=N'time' THEN REPLACE(N''''+CONVERT(nvarchar(12), prv.[value], 114)+N'''', N'.000', N'')
                 ELSE CAST(prv.[value] AS nvarchar(max)) END) AS boundary_varchar
    FROM sys.partition_functions AS pf
    INNER JOIN sys.partition_range_values AS prv ON pf.function_id=prv.function_id
    INNER JOIN sys.partition_schemes AS ps ON ps.function_id=pf.function_id
    LEFT JOIN sys.destination_data_spaces AS dds ON dds.partition_scheme_id=ps.data_space_id AND dds.destination_id=prv.boundary_id
    LEFT JOIN sys.filegroups AS fg ON dds.data_space_id=fg.data_space_id
    WHERE ps.data_space_id=@data_space_id)
    
UPDATE b
SET b.partition_number=val.boundary_id
FROM @boundaries AS b
LEFT JOIN val ON EXISTS (
    SELECT b.boundary_variant
    INTERSECT 
    SELECT val.[value]);



--- Anything to truncate after we've SPLIT RANGE?
IF (1 IN (@truncate_before_first_boundary, @truncate_after_last_boundary))
    EXECUTE dbo.sp_truncate_partitions
        @data_space_id=@data_space_id,
        @first_boundary=@first_boundary,
        @last_boundary=@last_boundary,
        @truncate_before_first_boundary=@truncate_before_first_boundary,
        @truncate_after_last_boundary=@truncate_after_last_boundary;


--- Remove (merge) partitions
--------------------------------------------------

SET @sql=NULL;

SELECT @sql=ISNULL(@sql+N'
', N'')+N'ALTER PARTITION FUNCTION '+QUOTENAME(@pf_name)+N'() MERGE RANGE ('+boundary_nvarchar+N');'
FROM (
    SELECT partition_number, boundary_nvarchar, boundary_variant,
           LEAD(boundary_variant, 1) OVER (ORDER BY partition_number) AS lead_boundary_variant,
           LAG (boundary_variant, 1) OVER (ORDER BY partition_number) AS lag_boundary_variant
    FROM @boundaries
    ) AS b
WHERE partition_number IS NOT NULL AND (
      @boundary_on_right=0 AND (boundary_variant>@last_boundary OR lead_boundary_variant<@first_boundary)
   OR @boundary_on_right=1 AND (lag_boundary_variant>@last_boundary OR boundary_variant<@first_boundary))
ORDER BY partition_number DESC;

IF (@sql IS NOT NULL) BEGIN;
    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;

    DELETE FROM @boundaries WHERE boundary_ordinal IS NULL;
END;




--- Set filegroups to read-write/read-only
--------------------------------------------------

SET @sql=NULL;

SELECT @sql=ISNULL(@sql+N'
', N'')+N'ALTER DATABASE CURRENT MODIFY FILEGROUP '+QUOTENAME(fg.[name])+(CASE
       WHEN b.boundary_variant<@read_write_before AND fg.is_read_only=1 THEN N' READ_WRITE'
       WHEN b.boundary_variant>@read_write_after AND fg.is_read_only=1 THEN N' READ_WRITE'
       WHEN b.boundary_variant<@read_only_before AND fg.is_read_only=0 THEN N' READ_ONLY'
       WHEN b.boundary_variant>@read_only_after AND fg.is_read_only=0 THEN N' READ_ONLY' END)
FROM @boundaries AS b
INNER JOIN sys.filegroups AS fg ON b.[filegroup]=fg.[name]
WHERE b.boundary_variant<@read_only_before AND fg.is_read_only=0
   OR b.boundary_variant<@read_write_before AND fg.is_read_only=1
   OR b.boundary_variant>@read_only_after AND fg.is_read_only=0
   OR b.boundary_variant>@read_write_after AND fg.is_read_only=1

IF (@sql IS NOT NULL) BEGIN;
    PRINT @sql;
    EXECUTE sys.sp_executesql @sql;
END;

GO
