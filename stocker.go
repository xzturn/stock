package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"xzturn/clog"
	"xzturn/fintech"
)

////////////////////////////////////////////////////////////////////////////////

const (
	nasdaqInfoFile = ".stock.exchange.nasdaq.dat"
	sseInfoFile    = ".stock.exchange.sse.dat"
	szseInfoFile   = ".stock.exchange.szse.dat"
	hkexInfoFile   = ".stock.exchange.hkex.dat"

	sseListFile  = ".companylist.sse.csv"
	szseListFile = ".companylist.szse.csv"
	hkexListFile = ".companylist.hkex.csv"

	authFile    = ".auth.xueqiu.json"
	favlistFile = ".favlist.json"
)

const (
	us = iota // US Exchange: amex, nasdaq, nyse
	sh        // SH Exchange
	sz        // SZ Exchange
	hk        // HK Exchange
)

const (
	xq = iota // XueQiu
	yh        // Yahoo Finance
)

const (
	kComma     = ","
	kSemiColon = ";"
)

////////////////////////////////////////////////////////////////////////////////

var logger *clog.ConsoleLogger = clog.NewConsoleLogger("Stocker: ")

// common parameters
var debugFlag *bool = flag.Bool("debug", false, "debug mode, -debug to enable it, default false")
var srcType *string = flag.String("src", "xueqiu", "specify src type, 'xueqiu'/'yahoo' supported, default 'xueqiu'")
var fromTime *string = flag.String("from", "", "specify the from time 'yyyymmdd', default nil means no specify")
var toTime *string = flag.String("to", "", "specify the to time 'yyyymmdd', default nil means no specify")

// show info parameters
var forceFlag *bool = flag.Bool("force", false, "force re-parse flag, only used with show info params, default false")

// 1. show stock info by symbol
var hkStockTag *string = flag.String("hkstock", "", "specify the hk stock symbol to show the info, default nil means no specify")
var shStockTag *string = flag.String("shstock", "", "specify the sh stock symbol to show the info, default nil means no specify")
var szStockTag *string = flag.String("szstock", "", "specify the sz stock symbol to show the info, default nil means no specify")
var usStockTag *string = flag.String("usstock", "", "specify the us stock symbol to show the info, default nil means no specify")

// 2. show all stocks in a certain Exchange
var hkStocksFlag *bool = flag.Bool("hkstocks", false, "show all the stocks in the HK Exchange, -hkstocks to enable it, default false")
var shStocksFlag *bool = flag.Bool("shstocks", false, "show all the stocks in the SH Exchange, -shstocks to enable it, default false")
var szStocksFlag *bool = flag.Bool("szstocks", false, "show all the stocks in the SZ Exchange, -szstocks to enable it, default false")
var usStocksFlag *bool = flag.Bool("usstocks", false, "show all the stocks in the US Exchange (NYSE/NASDAQ/AMEX), -usstocks to enable it, default false")

// 3. show group info in a certain Exchange/List
var usSectorFlag *bool = flag.Bool("ussectors", false, "show the US Exchange sectors, -ussectors to enable it, default false")
var usIndustryFlag *bool = flag.Bool("usindustries", false, "show the US Exchange industries, -usindustries to enable it, default false")
var szIndustryFlag *bool = flag.Bool("szindustries", false, "show the SZ Exchange industries, -szindustries to enable it, default false")
var szRegionFlag *bool = flag.Bool("szregions", false, "show the SZ Exchange regions, -szregions to enable it, default false")
var szProvinceFlag *bool = flag.Bool("szprovinces", false, "show the SZ Exchange provinces, -szprovinces to enable it, default false")
var szCityFlag *bool = flag.Bool("szcities", false, "show the SZ Exchange cities, -szcities to enable it, default false")
var favGroupsTag *string = flag.String("favgroups", "", "specify the favlist file to show the groups info")

// 4. show group stocks in a certain Exchange/List
var usSectorStocks *string = flag.String("ussectorstocks", "", "show the sector stocks in US Exchange")
var usIndustryStocks *string = flag.String("usindustrystocks", "", "show the industry stocks in US Exchange")
var szIndustryStocks *string = flag.String("szindustrystocks", "", "show the industry stocks in SZ Exchange")
var szRegionStocks *string = flag.String("szregionstocks", "", "show the region stocks in SZ Exchange")
var szProvinceStocks *string = flag.String("szprovincestocks", "", "show the province stocks in SZ Exchange")
var szCityStocks *string = flag.String("szcitystocks", "", "show the city stocks in SZ Exchange")
var usSectorIndustryStocks *bool = flag.Bool("ussecindstocks", false, "show the sector.industry stocks in US Exchange, -ussecindstocks {sec} {ind}")
var usIndustrySectorStocks *bool = flag.Bool("usindsecstocks", false, "show the industry.sector stocks in US Exchange, -usindsecstocks {ind} {sec}")

// crawl
var listFlag *bool = flag.Bool("uslists", false, "crawl the US Exchange stock list, -uslists to enable it, default false")
var crawlSymbol *string = flag.String("crawl", "", "the symbol(s) to crawl from XueQiu or Yahoo")
var crawlFavlist *string = flag.String("crawlf", "", "crawl the favlist file specified symbols from XueQiu or Yahoo")
var crawlAllUsStocks *bool = flag.Bool("crawlusall", false, "crawl all the us stocks from XueQiu or Yahoo, default false")

// plot (crawl if needed)
// 1. plot or compare by input parameter
var plotSymbol *string = flag.String("plot", "", "plot the xueqiu or yahoo symbols, crawl if needed")
var plotWithIndex *string = flag.String("market", "", "used with -plot, specify stock market: us/sh/sz/hk")
var paramRF *float64 = flag.Float64("rf", 0.04, "used with -plot, specify risk free rate, (0,1)")

// 2. plot or compare by favlist file
var plotFavlist *string = flag.String("favlist", "", "specify the favlist, xueqiu only, crawl if needed")
var plotFavgroup *string = flag.String("group", "", "plot compare the favgroup stocks, xueqiu or yahoo, used with -favlist")

// 3. plot default indexes
var plotIndexes *bool = flag.Bool("indexes", false, "crawl and plot compare default global indexes, xueqiu or yahoo")

// 4. plot or compare by group in a certain Exchange
var usSectorPlot *string = flag.String("ussectorplot", "", "crawl and plot us stocks by sector, xueqiu only")
var usIndustryPlot *string = flag.String("usindustryplot", "", "crawl and plot us stocks by industry, xueqiu only")
var usSecindPlot *bool = flag.Bool("ussecindplot", false, "crawl and plot us stocks by sector->industry, xueqiu only")
var usSecindCmp *bool = flag.Bool("ussecindcmp", false, "crawl and plot compare us stocks by sector->industry, xueqiu or yahoo")
var usIndsecPlot *bool = flag.Bool("usindsecplot", false, "crawl and plot us stocks by industry->sector, xueqiu only")
var usIndsecCmp *bool = flag.Bool("usindseccmp", false, "crawl and plot compare us stocks by industry->sector, xueqiu or yahoo")
var szIndustryPlot *string = flag.String("szindustryplot", "", "crawl and plot sz stocks by industry, xueqiu only")
var szRegionPlot *string = flag.String("szregionplot", "", "crawl and plot sz stocks by region, xueqiu only")
var szProvincePlot *string = flag.String("szprovinceplot", "", "crawl and plot sz stocks by provice, xueqiu only")
var szCityPlot *string = flag.String("szcityplot", "", "crawl and plot sz stocks by city, xueqiu only")

// 5. plot all stocks in a certain Exchange
var usplotsFlag *bool = flag.Bool("usplots", false, "download then plot all the stocks in the us market, xueqiu only")
var shplotsFlag *bool = flag.Bool("shplots", false, "download then plot all the stocks in the sh market, xueqiu only")
var szplotsFlag *bool = flag.Bool("szplots", false, "download then plot all the stocks in the sz market, xueqiu only")
var hkplotsFlag *bool = flag.Bool("hkplots", false, "download then plot all the stocks in the hk market, xueqiu only")

// 6. analysis part
var usCorrAnalysis *string = flag.String("usanalyze", "", "specify the stock symbol to calculate the corr with all the stocks in the us market")
var startTime *string = flag.String("start", "", "specify the start time 'yyyymmdd', default nil means no specify")
var endTime *string = flag.String("end", "", "specify the end time 'yyyymmdd', default nil means no specify")

////////////////////////////////////////////////////////////////////////////////

func helperAnalyzer(tag int, opaque string) fintech.Analyzer {
	var a fintech.Analyzer = nil
	switch tag {
	case xq:
		a = fintech.NewXueQiuAnalyzer()
		params := strings.Split(opaque, kSemiColon)
		if err := a.(*fintech.XueQiuAnalyzer).SetAuth(params[0]); err != nil {
			logger.Error("SetAuth '%s' error: %v", params[0], err)
			return nil
		}
		if err := a.(*fintech.XueQiuAnalyzer).SetStartDate(params[1]); err != nil {
			logger.Error("SetStartDate '%s' error: %v", params[1], err)
			return nil
		}
		if err := a.(*fintech.XueQiuAnalyzer).SetEndDate(params[2]); err != nil {
			logger.Error("SetEndDate '%s' error: %v", params[2], err)
			return nil
		}
	case yh:
		a = fintech.NewYahooFinanceAnalyzer()
		params := strings.Split(opaque, kSemiColon)
		if err := a.(*fintech.YahooFinanceAnalyzer).SetStartDate(params[0]); err != nil {
			logger.Error("SetStartDate '%s' error: %v", params[0], err)
			return nil
		}
		if err := a.(*fintech.YahooFinanceAnalyzer).SetEndDate(params[1]); err != nil {
			logger.Error("SetEndDate '%s' error: %v", params[1], err)
			return nil
		}
	default:
		logger.Error("Unknown Analyzer Type: %d", tag)
	}
	return a
}

func getCAPMindices(tag, market int) []string {
	switch tag {
	case xq:
		return fintech.XQCompareIndex(market)
	case yh:
		return fintech.YHCompareIndex(market)
	}
	return nil
}

func helperCrawler(tag int, opaque string) fintech.Crawler {
	var c fintech.Crawler = nil
	switch tag {
	case xq:
		c = fintech.NewXueQiuCrawler()
		if err := c.(*fintech.XueQiuCrawler).SetAuth(opaque); err != nil {
			logger.Error("SetAuth from '%s' error: %v", opaque, err)
			return nil
		}
	case yh:
		c = fintech.NewYahooFinanceCrawler()
		params := strings.Split(opaque, kSemiColon)
		if err := c.(*fintech.YahooFinanceCrawler).SetFromDate(params[0]); err != nil {
			logger.Error("SetFromDate '%s' error: %v", params[0], err)
			return nil
		}
		if err := c.(*fintech.YahooFinanceCrawler).SetToDate(params[1]); err != nil {
			logger.Error("SetToDate '%s' error: %v", params[1], err)
			return nil
		}
	default:
		logger.Error("Unknown Crawler Type: %d", tag)
	}
	return c
}

func crawlSymbols(symbols []string, tag int, opaque, msg string) bool {
	if c := helperCrawler(tag, opaque); c != nil {
		var err error = nil
		if len(symbols) == 1 {
			err = c.Crawl(symbols[0])
		} else {
			err = fintech.Crawls(c, symbols)
		}
		if err != nil {
			logger.Error("Crawl %s error: %v", msg, err)
		} else {
			logger.Success("Crawl %s Done!", msg)
			return true
		}
	}
	return false
}

func dpsProcedure(p fintech.Parser, ifile, cfile string) bool {
	// Step1: try to deserialize from info file
	// if success just return, otherwise goto Step2
	if e := fintech.Deserialize(p, ifile); e != nil {
		logger.Printf("Deserialize '%s' error: %v, continue...", ifile, e)
		// Step2: try to parse existed csv file
		// if success goto Step3, otherwise return nil
		if e1 := p.Parse(cfile); e1 != nil {
			logger.Error("Parse '%s' error: %v - INVALID Parser!", cfile, e)
			return false
		} else if e2 := fintech.Serialize(p, ifile); e2 != nil {
			// Step3: serialize to info file
			logger.Printf("Serialize to '%s' error: %v, but VALID new Parser!", ifile, e2)
		} else {
			logger.Success("Serialized to '%s', and VALID new Parser!", ifile)
		}
	} else {
		logger.Success("Deserialized from '%s', and VALID old Parser!", ifile)
	}
	return true
}

func getParser(tag int) fintech.Parser {
	var p fintech.Parser = nil
	switch tag {
	case us:
		p = fintech.NewNasdaqParser()
		// Step1: try to deserialize from info file
		// if success just return
		if e := fintech.Deserialize(p, nasdaqInfoFile); e != nil {
			logger.Printf("Deserialize '%s' error: %v, continue...", nasdaqInfoFile, e)
			t := fintech.NewNasdaqProcessor()
			// Step2: try to re-parse csv files
			// if success goto Step5, otherwise goto Step3
			if e1 := t.DumpTo(nasdaqInfoFile); e1 != nil {
				logger.Printf("Parse nasdaq company lists error: %v, continue...", e1)
				// Step3: try to crawl csv files
				// if success goto Step4, otherwise return nil
				if e2 := t.FetchCompanyLists(); e2 != nil {
					logger.Error("FetchCompanyList error: %v - INVALID Parser", e2)
					return nil
				} else if e3 := t.DumpTo(nasdaqInfoFile); e3 != nil {
					// Step4: try to parse the newly crawled csv files,
					// if success goto Step5, otherwise return nil
					logger.Error("Re-Parse new nasdaq company lists error: %v - INVALID Parser", e2)
					return nil
				} else {
					logger.Success("Re-Parse new nasdaq company lists to '%s', continue...", nasdaqInfoFile)
				}
			} else {
				logger.Success("Re-Parse old nasdaq company lists to '%s', continue...", nasdaqInfoFile)
			}
			// Step5: try to re-deserialize from info file
			// if success just return, otherwise return nil
			if e := fintech.Deserialize(p, nasdaqInfoFile); e != nil {
				logger.Printf("Deserialize new '%s' error: %v - INVALID Parser", nasdaqInfoFile, e)
				return nil
			} else {
				logger.Success("Deserialize from '%s', and VALID new Parser", nasdaqInfoFile)
			}
		} else {
			logger.Success("Deserialized from '%s', and VALID old Parser!", nasdaqInfoFile)
		}
	case sh:
		p = fintech.NewSseParser()
		if !dpsProcedure(p, sseInfoFile, sseListFile) {
			return nil
		}
	case sz:
		p = fintech.NewSzseParser()
		if !dpsProcedure(p, szseInfoFile, szseListFile) {
			return nil
		}
	case hk:
		p = fintech.NewHkexParser()
		if !dpsProcedure(p, hkexInfoFile, hkexListFile) {
			return nil
		}
	}
	return p
}

func psProcedure(p fintech.Parser, ifile, cfile string) bool {
	if e := p.Parse(cfile); e != nil {
		logger.Error("Parse '%s' error: %v - INVALID Parser", cfile, e)
		return false
	}
	if e := fintech.Serialize(p, ifile); e != nil {
		logger.Printf("Serialize '%s' error: %v, but VALID Parser", ifile, e)
	}
	logger.Success("VALID new Parser!")
	return true
}

func genParser(tag int) fintech.Parser {
	var p fintech.Parser = nil
	switch tag {
	case us:
		p = fintech.NewNasdaqParser()
		t := fintech.NewNasdaqProcessor()
		if e := t.FetchCompanyLists(); e != nil {
			logger.Error("FetchCompanyLists error: %v - INVALID Parser", e)
			return nil
		}
		if e := t.DumpTo(nasdaqInfoFile); e != nil {
			logger.Error("Parse nasdaq company lists error: %v - INVALID Parser", e)
			return nil
		}
		if e := fintech.Deserialize(p, nasdaqInfoFile); e != nil {
			logger.Error("Deserialize NasdaqParser error: %v - INVALID Parser", e)
			return nil
		}
		logger.Success("VALID new Parser!")
	case sh:
		p = fintech.NewSseParser()
		if !psProcedure(p, sseInfoFile, sseListFile) {
			return nil
		}
	case sz:
		p = fintech.NewSzseParser()
		if !psProcedure(p, szseInfoFile, szseListFile) {
			return nil
		}
	case hk:
		p = fintech.NewHkexParser()
		if !psProcedure(p, hkexInfoFile, hkexListFile) {
			return nil
		}
	}
	return p
}

func helperParser(tag int, force bool, msg string) fintech.Parser {
	var p fintech.Parser = nil
	if force {
		p = genParser(tag)
	} else {
		p = getParser(tag)
	}
	if p == nil {
		fmt.Println("No VALID Parser - %s", msg)
	}
	return p
}

func showAllStocks(tag int, force bool) string {
	if p := helperParser(tag, force, "cannot get all stocks info!"); p != nil {
		return p.ShowInfo()
	}
	return ""
}

func showStock(symbol string, tag int, force bool) string {
	if p := helperParser(tag, force, "cannot get stock info!"); p != nil {
		return p.Lookup(symbol)
	}
	return ""
}

func cleanSzseData() error {
	blob, err := ioutil.ReadFile(".szse.tsv")
	if err != nil {
		logger.Error("ReadFile error: %v", err)
		return err
	}
	cs := regexp.MustCompile(" ")
	cn := regexp.MustCompile("\r")
	ct := regexp.MustCompile("\t")
	cq := regexp.MustCompile(`"`)
	tmp := cn.Split(string(blob), -1)
	N := len(tmp)
	logger.Printf("#lines: %d", N)
	schema := ct.Split(tmp[0], -1)
	n := len(schema)
	lines := make([]string, 0)
	for i := 0; i < n; i++ {
		schema[i] = fmt.Sprintf(`"%s"`, cs.ReplaceAllString(schema[i], ""))
		fmt.Printf("[%d] %s\n", i, schema[i])
	}
	lines = append(lines, strings.Join(schema, ","))
	fmt.Printf("%s\n", lines[0])
	line := make([]string, n)
	for i := 1; i < N; i++ {
		t := cq.ReplaceAllString(tmp[i], "")
		tt := ct.Split(t, -1)
		if m := len(tt); m != n {
			fmt.Printf("[%d] %d != %d\n\t%s\n\t%s\n", i, m, n, t, tmp[i])
		} else {
			for k, v := range tt {
				line[k] = fmt.Sprintf(`"%s"`, v)
			}
			lines = append(lines, strings.Join(line, ","))
			cur := len(lines) - 1
			fmt.Printf("[%d] %s\n", cur, lines[cur])
		}
	}
	return ioutil.WriteFile(szseListFile, []byte(strings.Join(lines, "\n")), 0644)
}

func helperPlotter(tag int, opaque string) fintech.Plotter {
	var p fintech.Plotter = nil
	switch tag {
	case xq:
		params := strings.Split(opaque, kSemiColon)
		auth, from, to := params[0], params[1], params[2]
		p = fintech.NewXueQiuPlotter()
		if err := p.(*fintech.XueQiuPlotter).SetAuth(auth); err != nil {
			logger.Error("SetAuth from '%s' error: %v", auth, err)
			return nil
		}
		if err := p.(*fintech.XueQiuPlotter).SetFromDate(from); err != nil {
			logger.Error("SetFromDate '%s' error: %v", from, err)
			return nil
		}
		if err := p.(*fintech.XueQiuPlotter).SetToDate(to); err != nil {
			logger.Error("SetToDate '%s' error: %v", to, err)
			return nil
		}
	case yh:
		params := strings.Split(opaque, kSemiColon)
		from, to := params[0], params[1]
		p = fintech.NewYahooFinancePlotter()
		if err := p.(*fintech.YahooFinancePlotter).SetFromDate(from); err != nil {
			logger.Error("SetFromDate '%s' error: %v", from, err)
			return nil
		}
		if err := p.(*fintech.YahooFinancePlotter).SetToDate(to); err != nil {
			logger.Error("SetFromDate '%s' error: %v", to, err)
			return nil
		}
	default:
		logger.Error("Unknown Plotter Type: %d", tag)
	}
	return p
}

func setWorkDir(names ...string) error {
	c := "."
	if t, e := filepath.Abs(c); e == nil {
		c = t
	}
	w := filepath.Join(c, filepath.Join(names...))
	if err := os.MkdirAll(w, 0755); err != nil {
		return err
	}
	return os.Chdir(w)
}

func plotAllStocks(src, tag int, force bool, opaque, stag string) {
	if src != xq {
		logger.Error("Only '-src xueqiu' is supported!")
		return
	}

	ps := helperParser(tag, force, "cannot get all stocks list!")
	if ps == nil {
		return
	}

	if err := setWorkDir(stag); err != nil {
		logger.Error("SetWorkDir '%s' error: %v", stag, err)
		return
	}

	p := helperPlotter(src, opaque)
	if p == nil {
		return
	}

	if err := fintech.Plots(p, ps.Symbols(), ps.Snames()); err != nil {
		logger.Error("Plot '%s' stocks error: %v", stag, err)
	} else {
		logger.Success("Plot '%s' stocks Done!", stag)
	}
}

func plotGroupStocks(src, tag, gtag int, force bool, key, opaque, stag, sgtag string) {
	if src != xq {
		logger.Error("Only '-src xueqiu' is supported!")
		return
	}

	var ss, nn []string = nil, nil
	var ps fintech.Parser = nil
	switch tag {
	case us:
		ps = helperParser(tag, force, "cannot get us group stocks list!")
		switch gtag {
		case 0:
			ss = ps.(*fintech.NasdaqParser).SectorSymbols(key)
		case 1:
			ss = ps.(*fintech.NasdaqParser).IndustrySymbols(key)
		}
	case sz:
		ps = helperParser(tag, force, "cannot get sz group stocks list!")
		switch gtag {
		case 0:
			ss = ps.(*fintech.SzseParser).IndustrySymbols(key)
		case 1:
			ss = ps.(*fintech.SzseParser).RegionSymbols(key)
		case 2:
			ss = ps.(*fintech.SzseParser).ProvinceSymbols(key)
		case 3:
			ss = ps.(*fintech.SzseParser).CitySymbols(key)
		}
	}

	if ss == nil {
		return
	}
	nn = fintech.GetNames(ps, ss)

	if err := setWorkDir(stag, sgtag); err != nil {
		logger.Error("SetWorkDir error: %v", err)
		return
	}

	p := helperPlotter(src, opaque)
	if p == nil {
		return
	}

	if err := fintech.Plots(p, ss, nn); err != nil {
		logger.Error("Plot '%s.%s' stocks error: %v", stag, sgtag, err)
	} else {
		logger.Success("Plot '%s.%s' stocks Done!", stag, sgtag)
	}
}

func plotSubGroupStocks(src, tag, gtag int, force bool, key1, key2, opaque, stag, sgtag, ssgtag string) {
	if src != xq {
		logger.Error("Only '-src xueqiu' is supported!")
		return
	}

	var ss, nn []string = nil, nil
	var ps fintech.Parser = nil
	switch tag {
	case us:
		ps = helperParser(tag, force, "cannot get us subgroup stocks list!")
		switch gtag {
		case 0:
			ss = ps.(*fintech.NasdaqParser).SectorIndustrySymbols(key1, key2)
		case 1:
			ss = ps.(*fintech.NasdaqParser).IndustrySectorSymbols(key1, key2)
		}
	}

	if ss == nil {
		return
	}
	nn = fintech.GetNames(ps, ss)

	if err := setWorkDir(stag, sgtag, ssgtag); err != nil {
		logger.Error("SetWorkDir error: %v", err)
		return
	}

	p := helperPlotter(src, opaque)
	if p == nil {
		return
	}

	if err := fintech.Plots(p, ss, nn); err != nil {
		logger.Error("Plot '%s.%s.%s' stocks error: %v", stag, sgtag, ssgtag, err)
	} else {
		logger.Success("Plot '%s.%s.%s' stocks Done!", stag, sgtag, ssgtag)
	}
}

func plotcmpSubGroupStocks(src, tag, gtag int, force bool, key1, key2, opaque, stag, sgtag, ssgtag string) {
	var ss []string = nil
	var ps fintech.Parser = nil
	var err error = nil

	switch tag {
	case us:
		ps = helperParser(tag, force, "cannot get us subgroup stocks list!")
		switch gtag {
		case 0:
			ss = ps.(*fintech.NasdaqParser).SectorIndustrySymbols(key1, key2)
		case 1:
			ss = ps.(*fintech.NasdaqParser).IndustrySectorSymbols(key1, key2)
		}
	}
	if ss == nil {
		return
	}

	if err = setWorkDir(stag, sgtag, ssgtag); err != nil {
		logger.Error("SetWorkDir error: %v", err)
		return
	}

	p := helperPlotter(src, opaque)
	if p == nil {
		return
	}

	switch src {
	case xq:
		err = p.(*fintech.XueQiuPlotter).Compare(ss)
	case yh:
		err = p.(*fintech.YahooFinancePlotter).Plot(strings.Join(ss, kComma), "")
	}

	if err != nil {
		logger.Error("Plot Compare '%s.%s.%s' stocks error: %v", stag, sgtag, ssgtag, err)
	} else {
		logger.Success("Plot Compare '%s.%s.%s' stocks Done!", stag, sgtag, ssgtag)
	}
}

func getXQsymbols(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logger.Error("ReadDir(%s) error: %v", dir, err)
		return nil
	}
	cxq := regexp.MustCompile(`^.xq.*.json$`)
	sbs := make([]string, 0)
	for _, nm := range files {
		name := nm.Name()
		if cxq.MatchString(name) {
			n := len(name)
			sbs = append(sbs, name[4:n-5])
		}
	}
	return sbs
}

func getYHsymbols(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logger.Error("ReadDir(%s) error: %v", dir, err)
		return nil
	}
	cyh := regexp.MustCompile(`^.yh.*.rds$`)
	sbs := make([]string, 0)
	for _, nm := range files {
		name := nm.Name()
		if cyh.MatchString(name) {
			n := len(name)
			sbs = append(sbs, name[4:n-4])
		}
	}
	return sbs
}

func parseTimeRange(ts, te, ms, me string, stype int) (string, string, error) {
	// only 'yyyymmdd' is supported
	s, e := ts, te
	if len(ts) > 0 {
		if _, err := fintech.ParseXQTime(ts); err != nil {
			logger.Error("%s '%s' format error: %v", ms, ts, err)
			return s, e, err
		}
		if stype == yh { // yyyy-mm-dd
			s = fintech.TimeXQ2YH(ts)
		}
	}
	if len(te) > 0 {
		if _, err := fintech.ParseXQTime(te); err != nil {
			logger.Error("%s '%s' format error: %v", me, te, err)
			return s, e, err
		}
		if stype == yh { // yyyy-mm-dd
			e = fintech.TimeXQ2YH(te)
		}
	}
	return s, e, nil
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	prog := os.Args[0]

	// parse command line parameters
	flag.Parse()

	// judge src type: xueqiu or yahoo, there maybe more in the future
	stype, stag := -1, ""
	switch *srcType {
	case "xueqiu":
		stype, stag = xq, "xq"
	case "yahoo":
		stype, stag = yh, "yh"
	}
	if stype == -1 {
		logger.Error("-src '%s' UNKONWN", *srcType)
		os.Exit(-1)
	}

	// yyyymmdd format only
	from, to, err := parseTimeRange(*fromTime, *toTime, "-from", "-to", stype)
	if err != nil {
		os.Exit(-1)
	}
	start, end, err := parseTimeRange(*startTime, *endTime, "-start", "-end", stype)
	if err != nil {
		os.Exit(-1)
	}

	// base work directory
	curdir := "."
	if t, e := filepath.Abs(curdir); e == nil {
		curdir = t
	}

	// some common params
	opaque, opaque2, opaque3, sep := "", "", "", ""
	switch stype {
	case xq:
		sep = kComma
		opaque = filepath.Join(curdir, authFile)
		opaque2 = strings.Join([]string{opaque, from, to}, kSemiColon)
		opaque3 = strings.Join([]string{opaque, start, end}, kSemiColon)
	case yh:
		sep = kSemiColon
		opaque = strings.Join([]string{from, to}, kSemiColon)
		opaque2 = opaque
		opaque3 = strings.Join([]string{start, end}, kSemiColon)
	}

	if *debugFlag {
		// debug mode: test XueQiu crawl 'BABA', and parse
		c := fintech.NewXueQiuCrawler()
		if err := c.SetAuth(filepath.Join(curdir, authFile)); err != nil {
			logger.Error("SetAuth(%s) error: %v", authFile, err)
			os.Exit(-1)
		}
		symbol := "BABA"
		if err := c.Crawl(symbol); err != nil {
			os.Exit(-2)
		}
		var xqsd fintech.XueQiuStockData
		if dfile, err := xqsd.Parse2CSV(fintech.XQFile(symbol), "", ""); err != nil {
			logger.Error("Parse downloaded '%s' error: %v", symbol, err)
			os.Exit(-3)
		} else {
			c.Success("%s dumped successfully!", dfile)
		}
	} else if *listFlag {
		// crawl & dump the US Exchange stock lists
		p := fintech.NewNasdaqProcessor()
		if err := p.FetchCompanyLists(); err != nil {
			logger.Error("FetchCompanyLists error: %v", err)
		} else if e := p.DumpTo(nasdaqInfoFile); e != nil {
			logger.Error("Dump '%s' error: %v", nasdaqInfoFile, err)
		} else {
			logger.Success("Download and DumpTo '%s'", nasdaqInfoFile)
		}
	} else if *crawlSymbol != "" {
		// crawl by symbol(s) from XueQiu or Yahoo
		crawlSymbols(strings.Split(*crawlSymbol, sep), stype, opaque, "symbol(s)")
	} else if *crawlFavlist != "" {
		// crawl by favlist file from XiuQiu or Yahoo
		p := fintech.NewFavParser()
		if err = p.Parse(*crawlFavlist); err != nil {
			logger.Error("Parse '%s' error: %v", *crawlFavlist, err)
		} else if stype == yh {
			crawlSymbols([]string{strings.Join(p.YHSymbols(), kComma)}, stype, opaque, *crawlFavlist)
		} else {
			crawlSymbols(p.Symbols(), stype, opaque, *crawlFavlist)
		}
	} else if *crawlAllUsStocks {
		if p := genParser(us); p != nil {
			tdir := filepath.Join(curdir, "us")
			switch stype {
			case xq:
				tdir = filepath.Join(tdir, "xueqiu")
			case yh:
				tdir = filepath.Join(tdir, "yahoo")
			}
			if err = os.MkdirAll(tdir, 0755); err != nil {
				logger.Error("Mkdir %s error: %v", tdir, err)
				os.Exit(-1)
			}
			if err = os.Chdir(tdir); err != nil {
				logger.Error("Chdir %s error: %v", tdir, err)
				os.Exit(-1)
			}
			crawlSymbols(p.Symbols(), stype, opaque, "all us stocks")
		}
	} else if *usStocksFlag {
		// show all stocks in US Exchange
		fmt.Printf("%s", showAllStocks(us, *forceFlag))
	} else if *shStocksFlag {
		// show all stocks in SH Exchange
		fmt.Printf("%s", showAllStocks(sh, *forceFlag))
	} else if *szStocksFlag {
		// show all stocks in SZ Exchange
		fmt.Printf("%s", showAllStocks(sz, *forceFlag))
	} else if *hkStocksFlag {
		// show all stocks in HK Exchange
		fmt.Printf("%s", showAllStocks(hk, *forceFlag))
	} else if *usSectorFlag {
		// show all sectors in US Exchange
		if p := helperParser(us, *forceFlag, "cannot get us stock sectors info!"); p != nil {
			fmt.Printf("%s", p.(*fintech.NasdaqParser).SectorDetailInfos())
		}
	} else if *usIndustryFlag {
		// show all industries in US Exchange
		if p := helperParser(us, *forceFlag, "cannot get us stock industries info!"); p != nil {
			fmt.Printf("%s", p.(*fintech.NasdaqParser).IndustryDetailInfos())
		}
	} else if *szIndustryFlag {
		// show all industries in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz stock industries info!"); p != nil {
			fmt.Printf("%s", p.(*fintech.SzseParser).ShowIndustryInfo())
		}
	} else if *szRegionFlag {
		// show all regions in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz stock regions info!"); p != nil {
			fmt.Printf("%s", p.(*fintech.SzseParser).ShowRegionInfo())
		}
	} else if *szProvinceFlag {
		// show all provinces in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz stock provinces info!"); p != nil {
			fmt.Printf("%s", p.(*fintech.SzseParser).ShowProvinceInfo())
		}
	} else if *szCityFlag {
		// show all cities in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz stock cities info!"); p != nil {
			fmt.Printf("%s", p.(*fintech.SzseParser).ShowCityInfo())
		}
	} else if *usSectorStocks != "" {
		// show all stocks by sector in US Exchange
		if p := helperParser(us, *forceFlag, "cannot get us sector stocks!"); p != nil {
			if slist := p.(*fintech.NasdaqParser).SectorSymbols(*usSectorStocks); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *usIndustryStocks != "" {
		// show all stocks by industry in US Exchange
		if p := helperParser(us, *forceFlag, "cannot get us industry stocks!"); p != nil {
			if slist := p.(*fintech.NasdaqParser).IndustrySymbols(*usIndustryStocks); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *szIndustryStocks != "" {
		// show all stocks by industry in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz industry stocks!"); p != nil {
			if slist := p.(*fintech.SzseParser).IndustrySymbols(*szIndustryStocks); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *szRegionStocks != "" {
		// show all stocks by region in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz region stocks!"); p != nil {
			if slist := p.(*fintech.SzseParser).RegionSymbols(*szRegionStocks); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *szProvinceStocks != "" {
		// show all stocks by province in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz province stocks!"); p != nil {
			if slist := p.(*fintech.SzseParser).ProvinceSymbols(*szProvinceStocks); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *szCityStocks != "" {
		// show all stocks by city in SZ Exchange
		if p := helperParser(sz, *forceFlag, "cannot get sz city stocks!"); p != nil {
			if slist := p.(*fintech.SzseParser).CitySymbols(*szCityStocks); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *usSectorIndustryStocks {
		// show all stocks by sector.industry in US Exchange
		if flag.NArg() != 2 {
			logger.Error("%s -ussecindstocks {paramSector} {paramIndustry}", prog)
			os.Exit(-1)
		}
		if p := helperParser(us, *forceFlag, "cannot get us sector.industry stocks!"); p != nil {
			if slist := p.(*fintech.NasdaqParser).SectorIndustrySymbols(flag.Arg(0), flag.Arg(1)); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *usIndustrySectorStocks {
		// show all stocks by industry.sector in US Exchange
		if flag.NArg() != 2 {
			logger.Error("%s -usindsecstocks {paramIndustry} {paramSector}", prog)
			os.Exit(-1)
		}
		if p := helperParser(us, *forceFlag, "cannot get us industry.sector stocks!"); p != nil {
			if slist := p.(*fintech.NasdaqParser).IndustrySectorSymbols(flag.Arg(0), flag.Arg(1)); slist != nil {
				fmt.Printf("%s", strings.Join(slist, ","))
			}
		}
	} else if *favGroupsTag != "" {
		// show all stocks by fav group
		p := fintech.NewFavParser()
		if err = p.Parse(*favGroupsTag); err != nil {
			logger.Error("Parse '%s' error: %v", *favGroupsTag, err)
		} else {
			fmt.Printf("%s", p.GroupInfos())
		}
	} else if *usStockTag != "" {
		// show us stock info
		fmt.Printf("%s", showStock(*usStockTag, us, *forceFlag))
	} else if *shStockTag != "" {
		// show sh stock info
		fmt.Printf("%s", showStock(*shStockTag, sh, *forceFlag))
	} else if *szStockTag != "" {
		// show sz stock info
		fmt.Printf("%s", showStock(*szStockTag, sz, *forceFlag))
	} else if *hkStockTag != "" {
		// show hk stock info
		fmt.Printf("%s", showStock(*hkStockTag, hk, *forceFlag))
	} else if *usCorrAnalysis != "" {
		// analyze the correlation of a 'stock' with all the us market stocks
		var tdir string
		var sbs []string = nil
		switch stype {
		case xq:
			tdir = filepath.Join(curdir, "us", "xueqiu")
			sbs = getXQsymbols(tdir)
		case yh:
			tdir = filepath.Join(curdir, "us", "yahoo")
			sbs = getYHsymbols(tdir)
		}
		if sbs == nil {
			os.Exit(-1)
		}
		if err = os.Chdir(tdir); err != nil {
			logger.Error("Chdir %s error: %v", tdir, err)
			os.Exit(-1)
		}
		if a := helperAnalyzer(stype, opaque3); a != nil {
			fintech.Analyze(a, sbs, getCAPMindices(stype, us), *usCorrAnalysis, 100, *paramRF)
		}
	} else if *plotSymbol != "" {
		// plot xueqiu or yahoo symbols
		//   xueqiu: s1,s2,s3,s4
		//   yahoo:  s1,s2,s3;s4,s5;s6,s7,s8,s9
		symbols := strings.Split(*plotSymbol, sep)
		if p := helperPlotter(stype, opaque2); p != nil {
			switch *plotWithIndex {
			case "us":
				p.SetCompareIndices(us)
				p.SetRiskFreeRate(*paramRF)
			case "sh":
				p.SetCompareIndices(sh)
				p.SetRiskFreeRate(*paramRF)
			case "sz":
				p.SetCompareIndices(sz)
				p.SetRiskFreeRate(*paramRF)
			case "hk":
				p.SetCompareIndices(hk)
				p.SetRiskFreeRate(*paramRF)
			}
			if len(symbols) == 1 {
				err = p.Plot(symbols[0], "")
			} else {
				err = fintech.Plots(p, symbols, nil)
				if stype == xq {
					// xq will do the extra comparison
					p.(*fintech.XueQiuPlotter).Compare(symbols)
				}
			}
			if err != nil {
				logger.Error("Plot error: %v", err)
			} else {
				logger.Success("Plot '%s' Done!", *plotSymbol)
			}
		}
	} else if *plotIndexes {
		// crawl and plot compare the default global indexes, xueqiu or yahoo
		if p := helperPlotter(stype, opaque2); p != nil {
			if err = setWorkDir("indexes", stag); err != nil {
				logger.Error("Set WorkDir error: %v", err)
				os.Exit(-1)
			}
			switch stype {
			case xq:
				err = p.(*fintech.XueQiuPlotter).PlotGlobalIndexes()
			case yh:
				err = p.(*fintech.YahooFinancePlotter).PlotGlobalIndexes()
			}
			if err != nil {
				logger.Error("Plot Global Indexes error: %v", err)
			} else {
				logger.Success("Plot Global Indexes Done!")
			}
		}
	} else if *plotFavlist != "" {
		ps := fintech.NewFavParser()
		if err = ps.Parse(*plotFavlist); err != nil {
			logger.Error("Parse '%s' error: %v", *plotFavlist, err)
			os.Exit(-1)
		}
		p := helperPlotter(stype, opaque2)
		if p == nil {
			os.Exit(-1)
		}
		var ss, nn []string = nil, nil
		if *plotFavgroup != "" {
			// plot compare the symbols of a group in favlist, xueqiu or yahoo
			if stype == yh {
				ss, nn = ps.GroupYHSymbolAndNames(*plotFavgroup)
			} else {
				ss, nn = ps.GroupSymbolAndNames(*plotFavgroup)
			}
			if ss == nil {
				logger.Printf("No such group: '%s'", *plotFavgroup)
				os.Exit(0)
			}
			setWorkDir("fav", stag, *plotFavgroup)
			if len(ss) == 1 {
				err = p.Plot(ss[0], nn[0])
			} else if stype == yh {
				err = p.(*fintech.YahooFinancePlotter).Plot(strings.Join(ss, kComma), strings.Join(nn, "v.s."))
			} else { // xq as default
				err = p.(*fintech.XueQiuPlotter).Compare(ss)
			}
			if err != nil {
				logger.Error("Plot favlist by group '%s' error: %v", *plotFavgroup, err)
			} else {
				logger.Success("Plot favlist by group '%s' Done!", *plotFavgroup)
			}
		} else {
			// plot all the symbols in the favlist
			if stype != xq {
				logger.Error("You can only plot the favlist by XueQiu mode!")
				os.Exit(-1)
			}
			ss, nn = ps.SymbolAndNames()
			setWorkDir("fav", stag)
			if err = fintech.Plots(p, ss, nn); err != nil {
				logger.Error("Plot by '%s' error: %v", *plotFavlist, err)
			} else {
				logger.Success("Plot by '%s' Done!", *plotFavlist)
			}
		}
	} else if *usSectorPlot != "" {
		// plot the stocks specified by sector in US exchange, xueqiu only
		plotGroupStocks(xq, us, 0, *forceFlag, *usSectorPlot, opaque2, "us", *usSectorPlot)
	} else if *usIndustryPlot != "" {
		// plot the stocks specified by industry in US exchange, xueqiu only
		plotGroupStocks(xq, us, 1, *forceFlag, *usIndustryPlot, opaque2, "us", *usIndustryPlot)
	} else if *usSecindPlot {
		// plot the stocks by sector->industry in US exchange, xueqiu only
		if flag.NArg() != 2 {
			logger.Error("%s -ussecindplot {sector} {industry}", prog)
			os.Exit(-1)
		}
		plotSubGroupStocks(xq, us, 0, *forceFlag, flag.Arg(0), flag.Arg(1), opaque2, "us", flag.Arg(0), flag.Arg(1))
	} else if *usIndsecPlot {
		// plot the stocks by industry->sector in US exchange, xueqiu only
		if flag.NArg() != 2 {
			logger.Error("%s -usindsecplot {industry} {sector}", prog)
			os.Exit(-1)
		}
		plotSubGroupStocks(xq, us, 1, *forceFlag, flag.Arg(0), flag.Arg(1), opaque2, "us", flag.Arg(0), flag.Arg(1))
	} else if *usSecindCmp {
		// plot compare the stocks by sector->industry in US exchange, xueqiu or yahoo
		if flag.NArg() != 2 {
			logger.Error("%s -ussecindcmp {sector} {industry}", prog)
			os.Exit(-1)
		}
		plotcmpSubGroupStocks(stype, us, 0, *forceFlag, flag.Arg(0), flag.Arg(1), opaque2, "us", flag.Arg(0), flag.Arg(1))
	} else if *usIndsecCmp {
		// plot compare the stocks by industry->sector in US exchange, xueqiu or yahoo
		if flag.NArg() != 2 {
			logger.Error("%s -usindseccmp {industry} {sector}", prog)
			os.Exit(-1)
		}
		plotcmpSubGroupStocks(stype, us, 1, *forceFlag, flag.Arg(0), flag.Arg(1), opaque2, "us", flag.Arg(0), flag.Arg(1))
	} else if *szIndustryPlot != "" {
		// plot the stocks specified by industry in SZ exchange, xueqiu only
		plotGroupStocks(xq, sz, 0, *forceFlag, *szIndustryPlot, opaque2, "sz", *szIndustryPlot)
	} else if *szRegionPlot != "" {
		// plot the stocks specified by region in SZ exchange, xueqiu only
		plotGroupStocks(xq, sz, 1, *forceFlag, *szRegionPlot, opaque2, "sz", *szRegionPlot)
	} else if *szProvincePlot != "" {
		// plot the stocks specified by province in SZ exchange, xueqiu only
		plotGroupStocks(xq, sz, 2, *forceFlag, *szProvincePlot, opaque2, "sz", *szProvincePlot)
	} else if *szCityPlot != "" {
		// plot the stocks specified by city in SZ exchange, xueqiu only
		plotGroupStocks(xq, sz, 3, *forceFlag, *szCityPlot, opaque2, "sz", *szCityPlot)
	} else if *usplotsFlag {
		// plot all the stocks in the US exchange, xueqiu only
		plotAllStocks(xq, us, *forceFlag, opaque2, "us")
	} else if *szplotsFlag {
		// plot all the stocks in the SZ exchange, xueqiu only
		plotAllStocks(xq, sz, *forceFlag, opaque2, "sz")
	} else if *shplotsFlag {
		// plot all the stocks in the SH exchange, xueqiu only
		plotAllStocks(xq, sh, *forceFlag, opaque2, "sh")
	} else if *hkplotsFlag {
		// plot all the stocks in the HK exchange, xueqiu only
		plotAllStocks(xq, hk, *forceFlag, opaque2, "hk")
	} else {
		// Default: plot compare some indexes
		dfname := "._default.index.json"
		p := fintech.NewFavParser()
		if err = p.Parse(dfname); err != nil {
			logger.Error("Parse '%s' error: %v", dfname, err)
			os.Exit(-1)
		}
		switch stype {
		case xq:
			symbols := p.Symbols()
			if crawlSymbols(symbols, stype, opaque, dfname) {
				if pt := helperPlotter(stype, opaque2); pt != nil {
					pt.(*fintech.XueQiuPlotter).Compare(symbols)
				}
			}
		case yh:
			symbols := strings.Join(p.YHSymbols(), kComma)
			if crawlSymbols([]string{symbols}, stype, opaque, dfname) {
				if pt := helperPlotter(stype, opaque2); pt != nil {
					pt.(*fintech.YahooFinancePlotter).Plot(symbols, "Indexes")
				}
			}
		}
	}
}
