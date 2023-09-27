DROP PROCEDURE sp_ReturnApplyList
GO
CREATE PROCEDURE sp_ReturnApplyList
	 @JsonTable NVARCHAR(100)
	, @JsonColumn NVARCHAR(100)
	, @JsonType INT
	, @ApplyList NVARCHAR(4000) OUTPUT
AS
BEGIN

DECLARE @SqlProperties AS NVARCHAR(500)
DECLARE @TempProperties TABLE(
[key] NVARCHAR(100)
,[type] INT
,CharLength INT
)

IF @JsonType = 5
	BEGIN
		SET @SqlProperties =	
			N'WITH CTE AS(
			SELECT 
				[key]
				, [type]
				, MAX(LEN([value])) CharLength
			FROM ' + @JsonTable + '
			CROSS APPLY OPENJSON(' + @JsonColumn + ')
			GROUP BY [key], [type]
			)
			SELECT 
				[key]
				,[type]
				,CharLength
			FROM CTE'
	END
IF @JsonType = 4
	BEGIN
		SET @SqlProperties =	
			N'WITH CTE AS(
			SELECT 
				jc2.[key]
				, jc2.[type]
				, MAX(LEN(jc2.[value])) CharLength
			FROM ' + @JsonTable + '
			CROSS APPLY OPENJSON(' + @JsonColumn + ')
			WITH([value] NVARCHAR(MAX) ''$'' AS JSON) jc
			CROSS APPLY OPENJSON(jc.[value], ''$'') jc2
			GROUP BY jc2.[key], jc2.[type]
			)
			SELECT 
				[key]
				,[type]
				,CharLength
			FROM CTE'
	END
INSERT INTO @TempProperties EXEC sp_executesql @SqlProperties
DECLARE @AltJsonCheck INT
SELECT @AltJsonCheck = COUNT(*) FROM @TempProperties;

IF @AltJsonCheck = 0 AND @JsonType = 4
	BEGIN
		SET @SqlProperties =
		N'SELECT 
			''value'' [value]
			, MIN([type]) [type]
			, MAX(LEN([value])) CharLength
		FROM ' + @JsonTable + '
		CROSS APPLY OPENJSON(' + @JsonColumn + ')'		
		INSERT INTO @TempProperties EXEC sp_executesql @SqlProperties
	END

SELECT 
	@ApplyList = 
		STRING_AGG(
			CASE 
				WHEN [type] = 1 THEN CONCAT('[',[key],'] NVARCHAR(',((CharLength/10)+1)*10,') ''$.',[key],'''')
				WHEN [type] = 2 THEN CONCAT('[',[key],'] INT ''$.',[key],'''')
				WHEN [type] = 3 THEN CONCAT('[',[key],'] BIT ''$.',[key],'''')
				WHEN [type] IN(4,5) THEN CONCAT('[',[key],'] NVARCHAR(MAX) ''$.',[key],''' AS JSON')
				ELSE NULL
			END
			, CHAR(13)+CHAR(10)+','
		)
		WITHIN GROUP (ORDER BY [key])
FROM @TempProperties
END