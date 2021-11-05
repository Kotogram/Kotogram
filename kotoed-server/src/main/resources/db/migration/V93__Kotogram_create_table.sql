CREATE TABLE Contacts (
  id        SERIAL NOT NULL PRIMARY KEY,
  denizenId INT REFERENCES denizen_unsafe(id),
  phoneNumber TEXT UNIQUE
);

CREATE TABLE TelegramBotUsers (
  id        SERIAL NOT NULL PRIMARY KEY,
  denizenId INT REFERENCES denizen_unsafe(id),
  chatId    BIGINT UNIQUE
);