package main
import (
        "flag"
        "fmt"
        "log"
        "os"
        "bufio"
        "io"
        "strings"
        "regexp"
        "os/exec"
         "io/ioutil"
         "runtime"
)


var (
        dbConfigFile      = flag.String("h", "", "db serverList path")
        sqlFile   = flag.String("s", "", "run sql file")
	serverName = flag.String("n", "", "Only run a Mysql server (OPTIONAL)")
	showTitle = flag.Bool("t", true, "Whether to display the title (OPTIONAL)")
)

type dbConfig struct {
  name string
  ip string
  port string
  user string
  password string
  result string
}

type Result struct {
    name string
    result string
} 

var dbConfigs map[string]*dbConfig
var results chan Result
var isAllServer = true

var worker = runtime.NumCPU()
func main() {
    // config cpu number
        runtime.GOMAXPROCS(worker)
        flag.Parse()
        log.SetFlags(log.Lshortfile | log.LstdFlags)
	 if *dbConfigFile == "" {
                fmt.Printf("Please enter the server list file path\n")
                return
        }

        if *sqlFile == "" {
                fmt.Printf("Please enter the run sql file\n")
                return
        }

        if *serverName != "" {
                isAllServer = false
        }
	

	dbConfigs = make(map[string]*dbConfig)

        getDBConfig()

    // 任务列表, 并发数目为CPU个数
    jobs := make(chan dbConfig, worker)
    // 标记完成
    dones := make(chan struct{}, worker)

    results = make(chan Result, 2 * worker)

    go addJob(dbConfigs,jobs)
    for i:=0; i<worker; i++ {
        go doJob(jobs, dones)
    }
    awaitForCloseResult(dones)

    //for name, db := range dbConfigs {
    //    fmt.Println( "===========  "+name+"  =============")
    //    fmt.Println(db.result)
    //}
    close(results)
}

func addJob(dbConfigs map[string]*dbConfig, jobs chan<- dbConfig) {
    for _, db := range dbConfigs {
        jobs <- *db
    }
    close(jobs)
}

func doJob(jobs <-chan dbConfig, dones chan<- struct{}) {
    for job := range jobs {
        job.Do()
    }
    dones <- struct{}{}
}

func awaitForCloseResult(dones <-chan struct{}) {
    working := worker
    done := false
    for {
        select {
	    case db := <-results:
		printResult(db);
            case <-dones:
                working -= 1
                if working <= 0 {
                       done = true
                }
            default:
                if done {
                    return
                }
        }
    }
}

func printResult(db Result) {
	if *showTitle {
		fmt.Println( "===========  "+db.name+"  =============")
		fmt.Println(db.result)
	} else {
		//fmt.Println(strings.LastIndex(db.result, "\n"))
		//fmt.Println(len(db.result))
		if len(db.result) > 0 {
			fmt.Println(db.result)
		}
	}
}


func (db *dbConfig) Do() {
        sql := "mysql -u"+db.user+" -p"+db.password+" -h"+db.ip+" -P"+db.port+" -N < "+ *sqlFile
        cmd := exec.Command("/bin/sh", "-c", sql)
        stdout, err := cmd.StdoutPipe()
        if err != nil {
		results <- Result{db.name,"StdoutPipe: " + err.Error()}
                return
        }

        stderr, err := cmd.StderrPipe()
        if err != nil {
		results <- Result{db.name,"StderrPipe: " + err.Error()}
                return
        }

        if err := cmd.Start(); err != nil {
		results <- Result{db.name,err.Error()}
                return
        }

        bytesErr, err := ioutil.ReadAll(stderr)
        if err != nil {
		results <- Result{db.name,"ReadAll stderr: " + err.Error()}
                return
        }

        if len(bytesErr) != 0 {
		results <- Result{db.name,string(bytesErr)}
                return
        }

        bytes, err := ioutil.ReadAll(stdout)
        if err != nil {
		results <- Result{db.name,"ReadAll" + err.Error()}
                return
        }
        if err := cmd.Wait(); err != nil {
		results <- Result{db.name,err.Error()}
                return
        }
	
	if *showTitle == false {
		index := len(bytes)-1
		if index >=0 {
			 bytes[index] = ' '
		} 
	}

	result := string(bytes)
        db.result  = result
	results <- Result{db.name,db.result}
        return
}


func println(o ...interface{}) {
    fmt.Println(o...)
}

func getDBConfig() {
        fin,err := os.Open(*dbConfigFile)
        if err != nil {
                fmt.Println(*dbConfigFile,err)
                return
        }
	defer fin.Close()
        br := bufio.NewReader(fin)
        re, _ := regexp.Compile("\\s+")
   for{
       //每次读取一行
       line , err := br.ReadString('\n')
       if err != nil || io.EOF == err{
           break
       }else{
           line = re.ReplaceAllString(line, " ")
           strSplit := strings.Split(line, " ")
           if(len(strSplit) < 5){
                fmt.Println(strSplit)
           } else {
		if(isAllServer || strSplit[0] == *serverName){
	                dbConfigs[strSplit[0]] = &dbConfig{strSplit[0],strSplit[1],strSplit[2],strSplit[3],strSplit[4],""}
		}
           }
       }
   }


}




