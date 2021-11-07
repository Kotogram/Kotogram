package org.blackteam.kotogram

import com.github.kotlintelegrambot.bot
import com.github.kotlintelegrambot.dispatch
import com.github.kotlintelegrambot.dispatcher.command
import com.github.kotlintelegrambot.dispatcher.contact
import com.github.kotlintelegrambot.entities.ChatId
import com.github.kotlintelegrambot.entities.KeyboardReplyMarkup
import com.github.kotlintelegrambot.entities.ReplyKeyboardRemove
import com.github.kotlintelegrambot.entities.keyboard.KeyboardButton
import io.vertx.core.json.JsonObject
import org.jetbrains.research.kotoed.config.Config
import org.jetbrains.research.kotoed.database.tables.Contacts
import org.jetbrains.research.kotoed.util.database.KotoedDataSource
import org.jetbrains.research.kotoed.util.database.primaryKeyField
import org.jooq.SQLDialect.*
import org.jooq.impl.DSL
import java.sql.DriverManager
import kotlin.reflect.KClass
import org.jetbrains.research.kotoed.db.DatabaseVerticle
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.jetbrains.research.kotoed.data.notification.CurrentNotificationsQuery
import org.jetbrains.research.kotoed.database.enums.NotificationStatus
import org.jetbrains.research.kotoed.database.tables.records.ContactsRecord
import org.jetbrains.research.kotoed.database.tables.records.NotificationRecord
import org.jetbrains.research.kotoed.database.tables.records.ProfileRecord
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.jetbrains.research.kotoed.eventbus.Address
import org.jetbrains.research.kotoed.util.*
import javax.sql.DataSource
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.kotlin.resolve.calls.smartcasts.IdentifierInfo
import org.jetbrains.research.kotoed.database.Tables
import org.jetbrains.research.kotoed.database.tables.Telegrambotusers
import org.jooq.*
import org.jooq.exception.DataAccessException
import java.sql.SQLException
import java.util.function.Consumer

fun telegramBot()
{
    val bot = bot {

        token = org.blackteam.kotogram.createToken()

            //val ds: DataSource = HikariDataSource().apply {
            //this.username = Config.Debug.Database.User
            //this.password = Config.Debug.Database.Password
            //this.jdbcUrl = Config.Debug.Database.Url

            val userName = Config.Debug.Database.User
            val password = Config.Debug.Database.Password
            val url = Config.Debug.Database.Url

            dispatch {

                command("start") {


                    //val result = bot.sendMessage(chatId = ChatId.fromId(update.message!!.chat.id), text = "Привет!")
                    val keyboardMarkup = KeyboardReplyMarkup(
                        keyboard = listOf(
                            listOf(
                                KeyboardButton(
                                    "Потвердить номер телефона",
                                    requestContact = true
                                )
                            )
                        ), resizeKeyboard = true
                    )
                    bot.sendMessage(
                        chatId = ChatId.fromId(message.chat.id),
                        text = "Привет, подтверди свой номер телефона, что бы я мог присылать тебе уведомления от системы Котоед",
                        replyMarkup = keyboardMarkup
                    )


                }
                contact {
                    val phoneNumber: String = contact.phoneNumber
                    val chatNumber: Long = message.chat.id
                    try {
                        DriverManager.getConnection(url, userName, password).use { conn ->
                            val create: DSLContext = DSL.using(conn, POSTGRES)
                            //Найдём пользователя в базе данных котоеда по номеру телефона из telegram
                            val denizenIdList: List<Int> = create
                                    .selectFrom(Contacts.CONTACTS)
                                    .where(Contacts.CONTACTS.PHONENUMBER
                                        .eq(phoneNumber))
                                    .fetch()
                                    .getValues(Contacts.CONTACTS.DENIZENID)
                            if (denizenIdList.isEmpty()==false){
                                val denizenId = denizenIdList[0]
                                 // и внесём номер чата для этого пользователя в базу Котоеда
                                create.
                                insertInto(Telegrambotusers.TELEGRAMBOTUSERS,Telegrambotusers.TELEGRAMBOTUSERS.DENIZENID, Telegrambotusers.TELEGRAMBOTUSERS.CHATID )
                                    .values(denizenId,chatNumber)
                                    .execute()
                                bot.sendMessage(
                                    chatId = ChatId.fromId(message.chat.id),
                                    text = "Отлично, теперь я буду присылать тебе уведомления от системы Котоед!",
                                    replyMarkup = ReplyKeyboardRemove()
                                )
                            }
                            //Если пользователь не найден, то выведем сообщение
                            else{
                                bot.sendMessage(
                                chatId = ChatId.fromId(message.chat.id),
                                text = "Вы не указали номер телефона в профиле системы Котоед"
                                )
                            }

                        }

                    }
                    catch (e: DataAccessException) {
                        //Если номер чата для пользователя с таким номером уже был записан(ограничение unique),
                        // то выведем сообщение
                        bot.sendMessage(
                            chatId = ChatId.fromId(message.chat.id),
                            text = "Телеграм бот системы Котоед уже присылает уведомления по этому номеру"
                        )
                    }
                    catch (e: Exception) {
                        e.printStackTrace()
                    }


                }


            }

        }
        bot.startPolling()
}