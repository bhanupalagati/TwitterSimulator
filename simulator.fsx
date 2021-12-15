#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Uma.Uuid"
#r "nuget: MathNet.Numerics"
#r "nuget: Akka"
#r "nuget: Akka.Serialization.Hyperion"

#load "dataDefnitions.fs"

open System
open Akka.FSharp
open Akka.Remote
open MathNet.Numerics.Distributions
open System.Diagnostics
open DataDefnitions
open System.Text.RegularExpressions
open System.Security.Cryptography


let usersCount = int fsi.CommandLineArgs.[1]

// Preprocessing methods and helpers
let r = new Random()
let specialTags = 5
let mutable followers: List<int> = []
// Constructs a zipf's distibution on the given users which could be used for subscriptions and tweets
followers <- [for i in 1..usersCount -> int (Math.Ceiling((Zipf(1.0, 100).Probability (int i + 1) * float usersCount)))]
let zipfTweets = List.fold (fun x y -> x + y) 0 followers
let mutable tweetIds: List<string> = []

let commonHashTags = [|"Dosp"; "Twitter"; "Florida"; "Masters"; "Cyclone"; "USA"; "India"; "America"; "Algorithms"; "Elections"; "President"; "Rebirth"; "Genocide"; "Religion"|]

let processTweet(message) = 
    let hashTags = (" ", Regex(@"#\w+").Matches message) |> String.Join
    let mentions = (" ", Regex(@"@\w+").Matches message) |> String.Join
    (hashTags, mentions)

let constructHash arg1=
    let byteInfo = System.Text.Encoding.ASCII.GetBytes(arg1: string)
    (new SHA256Managed()).ComputeHash(byteInfo) |> BitConverter.ToString |> fun s -> s.Replace("-", "")

let constructTweets(userId: int, usersCount: int) = 
    let Id = DateTime.Now.Ticks |> string
    let mutable message  = String.Format("This is auto generated tweet with tweet Id {0}", Id)
    let mutable hashTag = ""
    let mutable mentions = ""
    let rand = r.Next(specialTags)
    for i in 0..rand do
        let r2 = r.Next(commonHashTags.Length-1)
        hashTag <- hashTag + "#" + commonHashTags.[r2] + " "
    for i in 0..rand do
        let r2 = r.Next(1, usersCount)
        mentions <- mentions + "@" + string(r2) + " "
    message <- message + hashTag + mentions
    let processedData = processTweet(message)
    let tweet = {Id = Id; message = message; senderId = string(userId); hashTags = fst processedData; mentions = snd processedData; asRetweet=false}
    tweet

let randomUniqueListGenerator(count, maxLimit) = 
    let rnd = System.Random()
    let initial = Seq.initInfinite (fun _ -> rnd.Next (1, maxLimit)) 
    initial
    |> Seq.distinct
    |> Seq.take(count)
    |> Seq.toList


// -------------------------------------------------------------------------
// AKKA Setup and configuration
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
                    port = 9010
                    hostname = localhost
                }
        }"

let system = System.create "twitterEngine" config


let totalTimer = new Stopwatch()
// Main simulator actor
let professor (mailbox: Actor<_>) = 
    let mutable responded = 0
    let mutable timer = new Stopwatch()
    let timeScale = 1.0
    let logger name = 
        responded <- 0
        timer.Stop()
        printfn "Performed the %s in %A ms" name (timer.Elapsed.TotalMilliseconds * timeScale)
        timer.Reset()
        timer.Start()
    let rec loop() = 
        actor {
            let! (msg: Commands) = mailbox.Receive()
            match msg with
            | StartRegister ->
                timer.Start()
                for i in 1..usersCount do
                    let userName = string i
                    let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + userName)
                    user <! Register(i, constructHash(userName))
            | RegisterSuccess -> 
                responded <- responded + 1
                if responded = usersCount then
                    logger "registers"
                    for i in 1..usersCount do
                        let user =  system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        user <! Login(i, constructHash(string i))
            | LoginSuccess -> 
                responded <- responded + 1
                if responded = usersCount then
                    logger "logins"
                    for i in 1..usersCount do
                        let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        let subs = randomUniqueListGenerator(followers.[i-1], usersCount)
                        user <! Subscribed(subs)
            | FollowedSuccess -> 
                let msg = "just a place holder"
                msg
            | SubscriptionSuccess -> 
                responded <- responded + 1
                if responded = usersCount then
                    logger "subscriptions"
                    for i in 1..usersCount do
                        let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        for j in 1..followers.[i-1] do 
                            let tweet_ = constructTweets(i, usersCount)
                            tweetIds <- [tweet_.Id] @ tweetIds
                            user <! Tweet(tweet_)
            | TweetSuccess -> 
                responded <- responded + 1
                if responded = zipfTweets then
                    logger (string zipfTweets + " tweets")
                    for i in 1..usersCount do
                        let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        user <! ReTweet tweetIds.[i-1]
            | ReTweetSuccess -> 
                responded <- responded + 1
                if responded = usersCount then
                    logger "retweets"
                    for i in 1..usersCount-1 do
                        let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        user <! DM("I am doing great from " + string (i - 1))
            | DMSuccess -> 
                responded <- responded + 1
                if responded = usersCount-1 then
                    logger "Direct Messages"
                    for i in 1..usersCount do
                        let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        user <! Query {searchOn = "hashTags"; operator="LIKE"; searchWith = "*"+commonHashTags.[0]+"*"}

            | QuerySuccess -> 
                responded <- responded + 1
                if responded = usersCount then
                    logger "Querys"
                    for i in 1..usersCount do
                        let user = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/" + string i)
                        user <! Logout(i, constructHash(string i))
            | LogoutSuccess -> 
                responded <- responded + 1
                if responded = usersCount then
                    logger "Logouts"
                    totalTimer.Stop()
                    printfn "All actions were performed in %A ms" (totalTimer.Elapsed.TotalMilliseconds * timeScale)
                    // mailbox.Self <! AllTweets
            | AllTweets ->
                // let tweets = getAllTweets()
                // for tweet in tweets do
                printfn "dummy"
            return! loop()
        }
    loop()

// ---------------------------------------------------------
// Remote communication and simulation triggering
let remoteCoor = system.ActorSelection("akka.tcp://remote-system@localhost:9009/user/coordinator")
remoteCoor <! CreateUsers usersCount
Threading.Thread.Sleep(10000)
totalTimer.Start()
let proff = spawn system "professor" professor
proff <! StartRegister

System.Console.ReadLine() |> ignore