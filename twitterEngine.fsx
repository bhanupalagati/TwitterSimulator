#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: MathNet.Numerics"
#r "nuget: Akka.Serialization.Hyperion"
#load "dataDefnitions.fs"

open Akka.FSharp
open Akka.Remote
open DataDefnitions
open System.Text.RegularExpressions
open System
open System.Data

//  ----------------------------------------------------------------
// In memory databases
let tweets = new DataTable()
let tweetsPKCol = new DataColumn("Id", typeof<string>)
tweets.Columns.Add(tweetsPKCol)
tweets.Columns.Add(new DataColumn("message", typeof<string>))
tweets.Columns.Add(new DataColumn("senderId", typeof<int32>))
tweets.Columns.Add(new DataColumn("hashTags", typeof<string>))
tweets.Columns.Add(new DataColumn("mentions", typeof<string>))
tweets.Columns.Add(new DataColumn("asRetweet", typeof<bool>))
tweets.PrimaryKey = [| tweetsPKCol |]

let users = new DataTable()
users.Columns.Add(new DataColumn("Id", typeof<string>))
users.Columns.Add(new DataColumn("password", typeof<string>))

// -----------------------------------------------------------------------
// Data retrival helpers
let buildTweets(selectRes: seq<DataRow>) =
    let tweetsResponse = selectRes |> Seq.map (fun ele -> {Id = string ele.["Id"]; message = string ele.["message"]; senderId = string ele.["senderId"]; hashTags = string ele.["hashTags"]; mentions = string ele.["mentions"]; asRetweet = bool.Parse(string ele.["asRetweet"])})
    tweetsResponse


let addTweets(tweet: tweet) = 
    lock(tweets) (fun () ->
        tweets.Rows.Add(tweet.Id, tweet.message, tweet.senderId, tweet.hashTags, tweet.mentions, tweet.asRetweet) |> ignore
    )

let queryTweets(query: query) =
    buildTweets(tweets.Select(String.Format("{0} {1} '{2}'", query.searchOn, query.operator, query.searchWith)))


let queryTweetsById(tweetId: string) = 
    buildTweets(tweets.Select(String.Format("Id = '{0}'", tweetId)))

let getAllTweets() = 
    buildTweets(tweets.Select())

// ---------------------------------------------------------------------------
//  Remote system configuration and spawning
let config =  
    Configuration.parse
        @"akka {
            actor.serializers{
                wire  = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
            }
            actor.serialization-bindings {
                ""System.Object"" = wire 
            }
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote {
                maximum-payload-bytes = 30000000 bytes
                helios.tcp {
                    port = 9009
                    hostname = localhost
                }
        }"

let system = System.create "remote-system" config

// User actor represents each user in the system
let userActor (mailbox: Actor<Message>) =
    let mutable userId = -100
    let mutable password = ""
    let mutable followers = []
    let mutable following = []
    let mutable isLoggedIn = false
    let mutable totalNotifications = 0
    let mutable privateKey = ""
    let coordinatorRef = select ("/user/coordinator") system
    let rec loop() = 
        actor {
            let! (msg:Message) = mailbox.Receive()
            let sender = mailbox.Sender()
            match msg with
            | Register (uid, pwd) -> 
                userId  <- uid
                password <- pwd
                sender <! RegisterSuccess
            | Login (uid, pwd) -> 
                if pwd = password then
                    isLoggedIn <- true
                    sender <! LoginSuccess
            | Logout (uid, pwd) ->
                if pwd = password then
                    isLoggedIn <- false
                    sender <! LogoutSuccess
            | NewFollower follower ->
                followers <- [follower] @ followers
            | Subscribed userids ->
                for id in userids do
                    coordinatorRef <! FollowerNotification(id, userId)
                following <- userids @ following
                sender <! SubscriptionSuccess
            | Tweet tweet -> 
                let res = addTweets tweet
                for uId in followers do
                    coordinatorRef <! TweetNotificationCarrer(string uId, tweet)
                for mention in Regex(@"@\w+").Matches tweet.mentions do
                    let m = string mention
                    coordinatorRef <! TweetNotificationCarrer(string m.[1..], tweet)
                sender <! TweetSuccess
            | ReTweet tweetId -> 
                let tweets = queryTweetsById(tweetId)
                for tweet in tweets do
                    for uId in followers do
                        coordinatorRef <! TweetNotificationCarrer(string uId, {tweet with asRetweet = true})
                sender <! ReTweetSuccess
            | DM message -> 
                // printfn "%A" message
                sender <! DMSuccess
            | TweetNotification tweet ->
                totalNotifications <- totalNotifications + 1
            | Query query -> 
                let tweets = queryTweets query
                sender <! QuerySuccess
            return! loop()
        }
    loop()

// --------------------------------------------------------------------
//  Coordinator Engine
let spawnUser userName = 
    spawn system userName userActor

let spawnUsers userCount = 
    for i in 1..userCount do
        spawnUser(string i) |> ignore
let coordinator(mailbox: Actor<Coordinator>) = 
    let rec loop() = 
        actor {
            let! (msg: Coordinator) = mailbox.Receive()
            match msg with
            | CreateUsers(usersCount) -> 
                spawnUsers(usersCount)
            | FollowerNotification(following, followedBy) -> 
                let user = select ("/user/"+ string following) system
                user <! NewFollower followedBy
            | TweetNotificationCarrer(notifyTo, tweet) -> 
                let user = select ("/user/"+ notifyTo) system
                user <! TweetNotification(tweet)
        }
    loop()

let coor = spawn system "coordinator" coordinator
System.Console.ReadLine() |> ignore
