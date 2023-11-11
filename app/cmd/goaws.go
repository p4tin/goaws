package main

import (
	"flag"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Admiral-Piett/goaws/app"

	log "github.com/sirupsen/logrus"

	"github.com/Admiral-Piett/goaws/app/conf"
	"github.com/Admiral-Piett/goaws/app/gosqs"
	"github.com/Admiral-Piett/goaws/app/router"
)

func main() {
	var filename string
	var debug bool
	var hotReload bool
	flag.StringVar(&filename, "config", "", "config file location + name")
	flag.BoolVar(&debug, "debug", false, "debug log level (default Warning)")
	flag.BoolVar(&hotReload, "hot-reload", false, "enable hot reload of config file for creation of new sqs queues and sns topics (default false)")
	flag.Parse()

	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	env := "Local"
	if flag.NArg() > 0 {
		env = flag.Arg(0)
	}

	configLoader := conf.NewConfigLoader(filename, env)

	if app.CurrentEnvironment.LogToFile {
		filename := app.CurrentEnvironment.LogFile
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			log.SetOutput(file)
		} else {
			log.Infof("Failed to log to file: %s, using default stderr", filename)
		}
	}

	r := router.New()

	quit := make(chan struct{}, 0)
	go gosqs.PeriodicTasks(1*time.Second, quit)

	//start config watcher, and make sure it's set before serving
	if hotReload {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		configLoader.StartWatcher(wg)
		wg.Wait()
	}

	if len(configLoader.Ports) == 1 {
		log.Warnf("GoAws listening on: 0.0.0.0:%s", configLoader.Ports[0])
		err := http.ListenAndServe("0.0.0.0:"+configLoader.Ports[0], r)
		log.Fatal(err)
	} else if len(configLoader.Ports) == 2 {
		go func() {
			log.Warnf("GoAws listening on: 0.0.0.0:%s", configLoader.Ports[0])
			err := http.ListenAndServe("0.0.0.0:"+configLoader.Ports[0], r)
			log.Fatal(err)
		}()
		log.Warnf("GoAws listening on: 0.0.0.0:%s", configLoader.Ports[1])
		err := http.ListenAndServe("0.0.0.0:"+configLoader.Ports[1], r)
		log.Fatal(err)
	} else {
		log.Fatal("Not enough or too many ports defined to start GoAws.")
	}
}
