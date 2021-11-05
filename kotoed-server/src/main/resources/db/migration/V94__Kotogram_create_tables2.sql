ALTER TABLE Contacts
ADD CONSTRAINT unique_id UNIQUE ( denizenId );

ALTER TABLE TelegramBotUsers
ADD CONSTRAINT unique_id2 UNIQUE ( denizenId );