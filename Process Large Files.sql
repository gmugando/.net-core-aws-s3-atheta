SET NOCOUNT ON

-- 20210403, CG - Version 1.0

--START script

USE [Doc_SARB_LargeDocs];

--declare some variables
DECLARE @SQL as nvarchar(MAX)
	, @DocumentId as uniqueidentifier
	, @SourceDatabase as nvarchar(500)
	, @hashSource as varbinary(max)
	, @hashDestination as varbinary(max)
	, @ErrorMessage as nvarchar(max)

--************************************************************************************************
--IMPORTANT: keep this line commented in production. Now is commented
--TRUNCATE TABLE [Doc_SARB_LargeDocuments].[dbo].[LargeDocumentsContents]
--*************************************************************************************************

--declare cursor to loop inside LargeDocs table
DECLARE LargeDocsLoop CURSOR FOR
SELECT 
	[DocumentId]
	, [SourceDatabase]
FROM [dbo].[LargeDocs]
-- add a where condition if you want to limit source dataset results
--WHERE ...

OPEN LargeDocsLoop;
FETCH NEXT FROM LargeDocsLoop 
	INTO @DocumentId, @SourceDatabase

WHILE @@FETCH_STATUS = 0
BEGIN
	--code inside BEGIN TRY will be rolled back in case of an error
	BEGIN TRY
		BEGIN TRANSACTION
			PRINT '*******************************************************'
			PRINT CONCAT('Processing Source db: ', @SourceDatabase, ', guid: ', @DocumentID)

			--Step 1: select record from [Doc_SARB_XX] database and insert data into [Doc_SARB_LargeDocuments]. Set verified field to false by default. It will be evaluated later
			SET @SQL = 'INSERT INTO [Doc_SARB_LargeDocuments].[dbo].[LargeDocumentsContents] ([DocumentId],[Contents],[SourceDatabase],[DateCreated] ,[Verified]) '
			SET @SQL = CONCAT(@SQL, 'SELECT [DocumentId], [Contents], ''', @SourceDatabase, ''', [DateCreated], 0  FROM [', @SourceDatabase,'].[dbo].[DocumentContents]')
			PRINT @SQL
			EXEC sp_executesql @sql

			--Step 2: verify checksum betwneen source and destination
			--get source hash
			SET @SQL = CONCAT('SELECT @hash = HASHBYTES(''md5'', CAST([Contents] AS VARBINARY(MAX)))  FROM [', @SourceDatabase,'].[dbo].[DocumentContents] WHERE [DocumentId] = ''', @DocumentID ,'''')
			PRINT @SQL
			EXEC sp_executesql @sql, N'@hash varbinary(max) OUTPUT', @hashSource OUTPUT
			PRINT CONCAT('Source hash: ', @hashSource)
			--get destination hash
			SELECT @hashDestination = HASHBYTES('md5', CAST([Contents] AS VARBINARY(MAX))) FROM [Doc_SARB_LargeDocuments].[dbo].[LargeDocumentsContents] WHERE [DocumentId] = '' + CAST(@DocumentID as nvarchar(max)) + ''
			PRINT CONCAT('Destination hash: ', @hashDestination)

			--Step 3: if checksums corresponds, set verified = true and delete source record
			IF @hashSource = @hashDestination
			BEGIN
				--Step 3.1: update verified field
				UPDATE [Doc_SARB_LargeDocuments].[dbo].[LargeDocumentsContents] SET Verified = 1 WHERE [DocumentId] = '' + CAST(@DocumentID as nvarchar(max)) + ''
				
				--Step 3.2: delete source record from [Doc_SARB_XX] database
				SET @SQL = CONCAT('DELETE FROM [', @SourceDatabase,'].[dbo].[DocumentContents] WHERE [DocumentId] = ''', @DocumentID ,'''')
				PRINT @SQL
				EXEC sp_executesql @sql
			END
			ELSE
			BEGIN
				PRINT 'Hashes are different. Source record has not been deleted';
			END

		COMMIT TRANSACTION
		PRINT 'Transaction committed'
	END TRY
	BEGIN CATCH
		IF XACT_STATE() <> 0
		BEGIN
			ROLLBACK TRANSACTION;
			PRINT 'Transaction rollbacked';
			SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'');
			RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT;
		END
	END CATCH

FETCH NEXT FROM LargeDocsLoop INTO @DocumentId, @SourceDatabase
END

CLOSE LargeDocsLoop;
DEALLOCATE LargeDocsLoop;

--END Script