#r "packages/log4net.2.0.5/lib/net40-full/log4net.dll"

let private hierarchy = log4net.LogManager.GetRepository() :?> log4net.Repository.Hierarchy.Hierarchy
let private patternLayout = log4net.Layout.PatternLayout()
patternLayout.ConversionPattern <- "%date [%thread] %-5level %logger %ndc - %message%newline"
patternLayout.ActivateOptions()
let private appender = log4net.Appender.FileAppender()
appender.AppendToFile <- true
appender.File <- "/tmp/fstorm.log"
appender.Layout <- patternLayout
appender.ActivateOptions()
hierarchy.Root.AddAppender(appender)
hierarchy.Root.Level <- log4net.Core.Level.Debug
hierarchy.Configured <- true

let private l4n = log4net.LogManager.GetLogger("StormIntegration")

let Debug = fun s -> s |> l4n.Debug;
let Info = fun s  -> s |> l4n.Info;
let Warn = fun s -> s |> l4n.Warn;
let Error = fun s -> s |> l4n.Error;
let Fatal = fun s -> s |> l4n.Fatal;

