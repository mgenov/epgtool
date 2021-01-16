package main

import (
	"encoding/csv"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	timespan "github.com/senseyeio/spaniel"
)

const (
	inDateLayout  = "20060102150405 -0700"
	outDateLayout = "2006-01-02T15:04:05Z"
)

var (
	dataDir          = flag.String("dataDir", "data", "data directory")
	sourceFileLimit  = flag.Int("sourceFileLimit", 5, "the maximum number of files to be read")
	sourceFilePrefix = flag.String("sourcePrefix", "CMS", "prefixed used to filter specific source files, e.g CMS-20210114")
	channelsFile     = flag.String("channelsFile", "channels.csv", "the mapping file for the channels")
	outputDir        = flag.String("outputDir", ".", "output directory where result will be written")
)

type source struct {
	ChannelList []channel   `xml:"channel"`
	ProgramList []programme `xml:"programme"`
}

type title struct {
	Lang string `xml:"lang,attr"`
	Name string `xml:",chardata"`
}

func (t *title) String() string {
	return fmt.Sprintf("%s (lang=%s)", t.Name, t.Lang)
}

type channel struct {
	ID   string `xml:"id,attr"`
	Name title  `xml:"display-name"`
	URL  string `xml:"url"`
}

// <programme start="20170701080000 +0300" stop="20170701100000 +0300" channel="Alfa">
//     <title lang="bg">~Tоб~@о ~C~B~@о, б~Jлга~@и</title>
//   </programme>
type programme struct {
	Start         string   `xml:"start,attr"`
	Stop          string   `xml:"stop,attr"`
	ChannelName   string   `xml:"channel,attr"`
	Description   title    `xml:"desc"`
	Title         []title  `xml:"title"`
	Credits       credits  `xml:"credits"`
	Date          string   `xml:"date"`
	Category      title    `xml:"category"`
	Country       []string `xml:"country"`
	EpisodeNumber string   `xml:"episode-num"`
}

type credits struct {
	Producers []string `xml:"producer"`
	Actors    []string `xml:"actor"`
}

type name struct {
	Name string `xml:",chardata"`
}

func (c *channel) String() string {
	return fmt.Sprintf("ID: %s, Name: %s, URL: %s", c.ID, c.Name.String(), c.URL)
}

type requestedChannel struct {
	ID   string
	Name string
}

type outputChannel struct {
	Name   string       `xml:"name,attr"`
	ID     string       `xml:"id,attr"`
	Events outputEvents `xml:"events"`
}

type outputEvents struct {
	Values []outputEvent `xml:"event"`
}
type outputEvent struct {
	ID                  string `xml:"id"`
	Name                string `xml:"name"`
	StartTime           string `xml:"time_from"`
	EndTime             string `xml:"time_till"`
	Perex               string `xml:"perex,omitempty"`
	Description         string `xml:"description,omitempty"`
	Actors              string `xml:"actors,omitempty"`
	Directors           string `xml:"directors,omitempty"`
	ProductionYear      string `xml:"production_year,omitempty"`
	ProductionCountries string `xml:"production_countries,omitempty"`
}

func listSourceFiles(dataDir string, filePrefix string, lastN int) ([]string, error) {
	var files []string
	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if filePrefix == "" || strings.HasPrefix(info.Name(), filePrefix) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(files)
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	if len(files) >= lastN {
		return files[0:lastN], nil
	}
	return files, nil
}

func readSources(files []string) []source {
	var result []source
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			panic(err)
		}

		var s source
		err = xml.NewDecoder(f).Decode(&s)
		if err != nil {
			panic(err)
		}

		result = append(result, s)
	}

	return result
}

func main() {
	flag.Parse()
	channels := readRequestedChannels("channels.csv")

	files, err := listSourceFiles(*dataDir, *sourceFilePrefix, *sourceFileLimit)
	if err != nil {
		log.Fatal(err)
	}

	sources := readSources(files)

	channelEvents := make(map[string][]programme)
	for _, s := range sources {
		for _, e := range s.ProgramList {
			v, ok := channelEvents[e.ChannelName]
			if !ok {
				channelEvents[e.ChannelName] = []programme{e}
			} else {
				channelEvents[e.ChannelName] = append(v, e)
			}
		}
	}
	fmt.Println("Source file count: ", len(files))
	fmt.Println("Channels: ", len(channels))
	fmt.Println("Events: ", len(channelEvents))
	writtenFiles := 0
	ids := make(map[string]programme)
	for _, channel := range channels {
		events, ok := channelEvents[channel.Name]
		if !ok {
			continue
		}
		outputChannel := &outputChannel{Events: outputEvents{Values: make([]outputEvent, 0)}}
		outputChannel.ID = channel.ID
		outputChannel.Name = channel.Name
		spans := timespan.Spans{}
		eventByStartTime := make(map[string]outputEvent)
		for _, event := range events {
			startTime, err := time.Parse(inDateLayout, event.Start)
			if err != nil {
				log.Fatalf("could not parse start time due: %v", err)
			}
			endTime, err := time.Parse(inDateLayout, event.Stop)
			if err != nil {
				log.Fatalf("could not parse start time due: %v", err)
			}

			id := fmt.Sprintf("%d", startTime.UTC().Unix())
			idc := fmt.Sprintf("%s-%s", id, event.ChannelName)

			v, ok := ids[idc]
			if !ok {
				ids[idc] = event
			} else {
				if v.ChannelName == event.ChannelName {
					continue
				}
			}

			actors := strings.Join(event.Credits.Actors, ", ")
			directors := strings.Join(event.Credits.Producers, ", ")
			countries := strings.Join(event.Country, ", ")

			var t = event.Title[0]

			for i, title := range event.Title {
				if title.Lang == "bg" {
					t = event.Title[i]
				}
			}

			overlaps := spans.IntersectionBetween(timespan.Spans{
				timespan.New(startTime, endTime),
			})

			if len(overlaps) > 0 {
				fmt.Println("collision detected")
				fmt.Printf("   %s channel=\"%s\" start=\"%s\" stop=\"%s\"\n", channel.ID, channel.Name, event.Start, event.Stop)
				existing, ok := eventByStartTime[endTime.UTC().Format(outDateLayout)]

				if ok {
					fmt.Println("   event desc: ", existing.Description)
				}
				fmt.Println("   skip desc: ", event.Description.Name)
				fmt.Println("   startTime: ", event.Start)
				fmt.Println("   endTime  : ", event.Stop)
				fmt.Println("event skipped")
				continue
			} else {
				spans = append(spans, timespan.New(startTime, endTime))
			}

			outputEvent := outputEvent{
				ID:                  id,
				Name:                t.Name,
				StartTime:           startTime.UTC().Format(outDateLayout),
				EndTime:             endTime.UTC().Format(outDateLayout),
				Perex:               event.Description.Name,
				Description:         event.Description.Name,
				Actors:              actors,
				Directors:           directors,
				ProductionYear:      event.Date,
				ProductionCountries: countries,
			}

			eventByStartTime[endTime.UTC().Format(outDateLayout)] = outputEvent

			outputChannel.Events.Values = append(outputChannel.Events.Values, outputEvent)
		}

		sort.Sort(byStartTime(outputChannel.Events.Values))

		if _, err := os.Stat(*outputDir); os.IsNotExist(err) {
			if err := os.MkdirAll(*outputDir, os.ModePerm); err != nil {
				log.Fatalf("unable to create output directory due: %v", err)
			}
		}

		outputFileName := filepath.Join(*outputDir, fmt.Sprintf("n_events_%s.xml", channel.ID))
		if err := marshalChannel(outputFileName, outputChannel); err != nil {
			log.Fatalf("could not write to output file '%s' due: %v", outputFileName, err)
		}
		writtenFiles++
	}

	log.Printf("Created files: %d\n", writtenFiles)

}

type byStartTime []outputEvent

func (a byStartTime) Len() int           { return len(a) }
func (a byStartTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byStartTime) Less(i, j int) bool { return a[i].ID < a[j].ID }

func marshalChannel(fileName string, channel *outputChannel) error {
	f, err := os.Create(fileName)
	if err != nil {

		return fmt.Errorf("unable to open output file due: %v", err)
	}
	defer f.Close()

	tmp := struct {
		outputChannel
		XMLName struct{} `xml:"channel"`
	}{outputChannel: *channel}

	enc := xml.NewEncoder(f)
	enc.Indent("  ", "    ")

	f.Write([]byte(xml.Header))

	if err := enc.Encode(tmp); err != nil {
		return fmt.Errorf("unable to marshall content due: %v", err)
	}

	return nil
}

func readRequestedChannels(fileName string) []requestedChannel {
	channelsFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("channels file doesn't exists")
	}
	defer channelsFile.Close()
	cr := csv.NewReader(channelsFile)

	channels, err := cr.ReadAll()
	if err != nil {
		log.Fatalf("could not read channels file due: %v", err)
	}

	result := make([]requestedChannel, 0)

	for _, rec := range channels {
		result = append(result, requestedChannel{ID: rec[0], Name: rec[1]})
	}
	return result
}
