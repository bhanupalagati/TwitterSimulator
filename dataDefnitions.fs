module DataDefnitions

type tweet = {Id: string; message: string; senderId: string; hashTags: string; mentions: string; asRetweet: bool}
type query = {searchOn: string; operator: string; searchWith: string}

type Message = 
    | Login of (int * string)
    | Logout of (int * string)
    | Tweet of tweet
    | Query of query
    | Register of (int * string)
    | NewFollower of int
    | Subscribed of List<int>
    | TweetNotification of tweet
    | ReTweet of string
    | DM of string

type Commands = 
    | StartRegister
    | RegisterSuccess
    | LoginSuccess
    | LogoutSuccess
    | FollowedSuccess
    | SubscriptionSuccess
    | TweetSuccess
    | QuerySuccess
    | AllTweets
    | ReTweetSuccess
    | DMSuccess

type Coordinator = 
    | CreateUsers of int
    | FollowerNotification of (int * int)
    | TweetNotificationCarrer of (string * tweet)

type remoteIntitator = 
    | Trigger of string