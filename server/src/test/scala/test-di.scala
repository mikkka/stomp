import java.util.Date
import java.text.DateFormat

class TwitterUserProfile(val userName: String) {
  override def toString = "@" + userName
}

case class Tweet(
  val tweeter: TwitterUserProfile,
  val message: String,
  val time: Date) {

  override def toString = "(" +
    DateFormat.getDateInstance(DateFormat.FULL).format(time) + ") " +
    tweeter + ": " + message
}

trait Tweeter {
  def tweet(message: String)
}

trait TwitterClientUIComponent {
  val ui: TwitterClientUI

  abstract class TwitterClientUI(val client: Tweeter) {
    def sendTweet(message: String) = client.tweet(message)
    def showTweet(tweet: Tweet): Unit
  }
}

trait TwitterLocalCacheComponent {
  val localCache: TwitterLocalCache

  trait TwitterLocalCache {
    def saveTweet(tweet: Tweet): Unit
    def history: List[Tweet]
  }
}

trait TwitterLocalCacheComponentImpl extends  TwitterLocalCacheComponent {
  class TwitterLocalCacheImpl(foo: String) extends TwitterLocalCache {
    private var tweets: List[Tweet] = Nil
    def saveTweet(tweet: Tweet) = tweets ::= tweet
    def history = tweets
  }
}

trait TwitterServiceComponent {
  val service: TwitterService

  trait TwitterService {
    def sendTweet(tweet: Tweet): Boolean
    def history: List[Tweet]
  }
}

trait TwitterClientComponent {
  self: TwitterClientUIComponent with
        TwitterLocalCacheComponent with
        TwitterServiceComponent =>

  val client: TwitterClient

  class TwitterClient(val user: TwitterUserProfile) extends Tweeter {
    def tweet(msg: String) = {
      val twt = new Tweet(user, msg, new Date)
      if (service.sendTweet(twt)) {
        localCache.saveTweet(twt)
        ui.showTweet(twt)
      }
    }
  }
}

class TextClient(userProfile: TwitterUserProfile)
    extends TwitterClientComponent
    with TwitterClientUIComponent
    with TwitterLocalCacheComponentImpl
    with TwitterServiceComponent {

  val client = new TwitterClient(userProfile)

  val ui = new TwitterClientUI(client) {
    def showTweet(tweet: Tweet) = println(tweet)
  }

  val localCache = new TwitterLocalCacheImpl(userProfile.toString)

  val service = new TwitterService() {
    def sendTweet(tweet: Tweet) = {
      println("Sending tweet to Twitter HQ")
      true
    }
    def history = List[Tweet]()
  }
}

val client = new TextClient(new TwitterUserProfile("BuckTrends"))
client.ui.sendTweet("My First Tweet. How's this thing work?")
client.ui.sendTweet("Is this thing on?")
client.ui.sendTweet("Heading to the bathroom...")
println("Chat history:")
client.localCache.history.foreach {t => println(t)}
