--Start the schema
CREATE SCHEMA twitter
GO

-----Create tables for twitter results-------

--Create the twitterDataTable
CREATE TABLE twitter.dataTable
(
	twitterID int IDENTITY(1,1) PRIMARY KEY NOT NULL,
	tweetText VARCHAR(280) NOT NULL,
	twitterUser VARCHAR(20) NOT NULL,
	timeSent varchar(50) NOT NULL,
	myLocationName VARCHAR(25) NOT NULL,
	twitterLocationName VARCHAR(25) NOT NULL,
	JSONstring NVARCHAR(MAX) NOT NULL 
);

--Create the microsoftApiTable
CREATE TABLE twitter.microApiTable
(
	sentimentScore NVARCHAR(MAX) NOT NULL,
	keyPhrases NVARCHAR(MAX) NOT NULL,
	twitterID int,
	CONSTRAINT FK_twitterID FOREIGN KEY (twitterID) REFERENCES twitter.dataTable(twitterID)
);


/*
DROP TABLE twitter.dataTable
DROP TABLE twitter.microApiTable
DROP SCHEMA twitter
*/


-------Stored procedures---------
/* Insert into dataTable */
CREATE OR ALTER PROCEDURE insertDataTable
	@tweetText varchar(280),
	@twitterUser varchar(20),
	@timeSent varchar(50),
	@myLocationName varchar(25),
	@twitterLocationName varchar(25),
	@JSONstring nvarchar(MAX)
AS
BEGIN
	DECLARE @DoYouExist int = 0
	SELECT
		@DoYouExist = COUNT(dt.twitterID)
	FROM
		twitter.dataTable dt
	WHERE
		dt.tweetText = @tweetText
		AND
		dt.twitterUser = @twitterUser
		AND
		dt.timeSent = @timeSent
		AND
		dt.myLocationName = @myLocationName
		AND 
		dt.twitterLocationName = @twitterLocationName
		AND 
		dt.JSONstring = @JSONstring
IF (@DoYouExist = 0)
BEGIN
	INSERT twitter.dataTable (tweetText, twitterUser, timeSent, myLocationName, twitterLocationName, JSONstring) 
	VALUES (@tweetText, @twitterUser, @timeSent, @myLocationName, @twitterLocationName, @JSONstring)
END
END;

--Insert into microAPITable
CREATE OR ALTER PROCEDURE insertMicroTable
	@sentiment nvarchar(MAX),
	@keyValue nvarchar(MAX),
	@twitterID int
AS
BEGIN
	DECLARE @DoYouExist int = 0
	SELECT
		@DoYouExist = COUNT(ma.twitterID)
	FROM
		twitter.microApiTable ma
	WHERE
		ma.sentimentScore = @sentiment
		AND
		ma.keyPhrases = @keyValue
IF (@DoYouExist = 0)
BEGIN
	INSERT twitter.microApiTable (sentimentScore, keyPhrases, twitterID)
	VALUES (@sentiment, @keyValue, @twitterID)
END
END;

