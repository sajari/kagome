// Copyright 2018 ikawaha
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// 	You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package juman

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"code.sajari.com/kagome/cmd/_dictool/splitfile"
	"code.sajari.com/kagome/internal/dic"
)

const (
	jumanMatrixDefFileName = "matrix.def"
	jumanCharDefFileName   = "char.def"
	jumanUnkDefFileName    = "unk.def"

	jumanDicArchiveFileName    = "juman.dic"
	jumanDicMorphFileName      = "morph.dic"
	jumanDicPOSFileName        = "pos.dic"
	jumanDicContentFileName    = "content.dic"
	jumanDicIndexFileName      = "index.dic"
	jumanDicConnectionFileName = "connection.dic"
	jumanDicCharDefFileName    = "chardef.dic"
	jumanDicUnkFileName        = "unk.dic"

	jumanMorphCsvColSize                    = 11
	jumanMrophRecordSurfaceIndex            = 0
	jumanMorphRecordLeftIDIndex             = 1
	jumanMorphRecordRightIDIndex            = 2
	jumanMorphRecordWeightIndex             = 3
	jumanMorphRecordPOSRecordStartIndex     = 4
	jumanMorphRecordOtherContentsStartIndex = 8

	jumanUnkRecordSize                    = 11
	jumanUnkRecordCategoryIndex           = 0
	jumanUnkRecordLeftIDIndex             = 1
	jumanUnkRecordRightIndex              = 2
	jumanUnkRecordWeigthIndex             = 3
	jumanUnkRecordOtherContentsStartIndex = 4
)

type jumanDic struct {
	Morphs       []dic.Morph
	POSTable     dic.POSTable
	Contents     [][]string
	Index        dic.IndexTable
	Connection   dic.ConnectionTable
	CharClass    []string
	CharCategory []byte
	InvokeList   []bool
	GroupList    []bool

	dic.UnkDic
}

type jumanDicPath struct {
	Morph      string
	POS        string
	Index      string
	Connection string
	CharDef    string
	Unk        string
}

type jumanMorphRecordSlice [][]string

func (p jumanMorphRecordSlice) Len() int           { return len(p) }
func (p jumanMorphRecordSlice) Less(i, j int) bool { return p[i][0] < p[j][0] }
func (p jumanMorphRecordSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func saveJumanDic(d *jumanDic, base string, archive bool) error {
	var zw *zip.Writer
	p := filepath.Join(base, jumanDicArchiveFileName)
	if archive {
		f, err := os.Create(p)
		if err != nil {
			return err
		}
		defer f.Close()
		zw = zip.NewWriter(f)
	} else {
		f, err := splitfile.Open(p, 10*1024*1024) // 10MB
		if err != nil {
			return err
		}
		defer f.Close()
		zw = zip.NewWriter(f)
		zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(out, flate.NoCompression)
		})
		if err != nil {
			return err
		}

	}

	if err := func() error {
		out, err := zw.Create(jumanDicMorphFileName)
		if err != nil {
			return err
		}
		_, err = dic.MorphSlice(d.Morphs).WriteTo(out)
		return err
	}(); err != nil {
		return err
	}

	if err := func() (e error) {
		out, err := zw.Create(jumanDicPOSFileName)
		if err != nil {
			return err
		}
		_, err = dic.POSTable(d.POSTable).WriteTo(out)
		return err
	}(); err != nil {
		return err
	}

	if err := func() error {
		out, err := zw.Create(jumanDicContentFileName)
		if err != nil {
			return err
		}
		_, err = dic.Contents(d.Contents).WriteTo(out)
		return err
	}(); err != nil {
		return err
	}

	if err := func() error {
		out, err := zw.Create(jumanDicIndexFileName)
		if err != nil {
			return err
		}
		_, err = d.Index.WriteTo(out)
		return err
	}(); err != nil {
		return err
	}

	if err := func() error {
		out, err := zw.Create(jumanDicConnectionFileName)
		if err != nil {
			return err
		}
		_, err = d.Connection.WriteTo(out)
		return err
	}(); err != nil {
		return err
	}

	if err := func() error {
		out, err := zw.Create(jumanDicCharDefFileName)
		if err != nil {
			return err
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(d.CharClass); err != nil {
			return err
		}
		if _, err := buf.WriteTo(out); err != nil {
			return err
		}
		if err := enc.Encode(d.CharCategory); err != nil {
			return err
		}
		if _, err := buf.WriteTo(out); err != nil {
			return err
		}
		if err := enc.Encode(d.InvokeList); err != nil {
			return err
		}
		if _, err := buf.WriteTo(out); err != nil {
			return err
		}
		if err := enc.Encode(d.GroupList); err != nil {
			return err
		}
		if _, err := buf.WriteTo(out); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	if err := func() error {
		out, err := zw.Create(jumanDicUnkFileName)
		if err != nil {
			return err
		}
		if _, err := d.UnkDic.WriteTo(out); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	return zw.Close()
}

func buildJumanDic(mecabPath, neologdPath string) (d *jumanDic, err error) {
	// Morphs, Contents, Index
	var files []string
	files, err = filepath.Glob(mecabPath + "/*.csv")
	if err != nil {
		return
	}
	var records jumanMorphRecordSlice
	for _, file := range files {
		if err = func() error {
			f, e := os.Open(file)
			if e != nil {
				return e
			}
			defer f.Close()
			r := csv.NewReader(f)
			r.Comma = ','
			for {
				rec, e := r.Read()
				if e == io.EOF {
					break
				} else if e != nil {
					return e
				} else if len(rec) != jumanMorphCsvColSize {
					return fmt.Errorf("invalid format csv: %v, %v", file, rec)
				}
				records = append(records, rec)
			}
			return nil
		}(); err != nil {
			return
		}
	}
	if err = func() error {
		if neologdPath == "" {
			return nil
		}
		f, e := os.Open(neologdPath)
		if e != nil {
			return e
		}
		defer f.Close()
		r := csv.NewReader(f)
		r.Comma = ','
		r.LazyQuotes = true
		for {
			rec, e := r.Read()
			if e == io.EOF {
				break
			} else if e != nil {
				return e
			} else if len(rec) != jumanMorphCsvColSize {
				return fmt.Errorf("invalid format csv: %v, %v", neologdPath, rec)
			}
			records = append(records, rec)
		}
		return nil
	}(); err != nil {
		return
	}

	sort.Sort(records)
	d = new(jumanDic)
	d.Morphs = make([]dic.Morph, 0, len(records))
	d.POSTable = dic.POSTable{
		POSs: make([]dic.POS, 0, len(records)),
	}
	d.Contents = make([][]string, 0, len(records))
	var (
		keywords []string
		posMap   = make(dic.POSMap)
	)
	for _, rec := range records {
		keywords = append(keywords, rec[jumanMrophRecordSurfaceIndex])
		var l, r, w int
		if l, err = strconv.Atoi(rec[jumanMorphRecordLeftIDIndex]); err != nil {
			return
		}
		if r, err = strconv.Atoi(rec[jumanMorphRecordRightIDIndex]); err != nil {
			return
		}
		if w, err = strconv.Atoi(rec[jumanMorphRecordWeightIndex]); err != nil {
			return
		}
		m := dic.Morph{LeftID: int16(l), RightID: int16(r), Weight: int16(w)}
		d.Morphs = append(d.Morphs, m)
		d.POSTable.POSs = append(d.POSTable.POSs, posMap.Add(
			rec[jumanMorphRecordPOSRecordStartIndex:jumanMorphRecordOtherContentsStartIndex]),
		)
		d.Contents = append(d.Contents, rec[jumanMorphRecordOtherContentsStartIndex:])
	}
	d.POSTable.NameList = posMap.List()

	if d.Index, err = dic.BuildIndexTable(keywords); err != nil {
		return
	}

	// ConnectionTable
	if r, c, v, e := loadJumanMatrixDefFile(mecabPath + "/" + jumanMatrixDefFileName); e != nil {
		err = e
		return
	} else {
		d.Connection.Row = r
		d.Connection.Col = c
		d.Connection.Vec = v
	}

	// CharDef
	if cc, cm, inv, grp, e := loadJumanCharClassDefFile(mecabPath + "/" + jumanCharDefFileName); e != nil {
		err = e
		return
	} else {
		d.CharClass = cc
		d.CharCategory = cm
		d.InvokeList = inv
		d.GroupList = grp
	}

	// Unk
	if records, e := loadJumanUnkFile(mecabPath + "/" + jumanUnkDefFileName); e != nil {
		err = e
		return
	} else {
		d.UnkIndex = make(map[int32]int32)
		d.UnkIndexDup = make(map[int32]int32)
		sort.Sort(jumanMorphRecordSlice(records))
		for _, rec := range records {
			catid := int32(-1)
			for id, cat := range d.CharClass {
				if cat == rec[jumanUnkRecordCategoryIndex] {
					catid = int32(id)
					break
				}
			}
			if catid < 0 {
				err = fmt.Errorf("unknown unk category: %v", rec[jumanUnkRecordCategoryIndex])
				return
			}
			if _, ok := d.UnkIndex[catid]; !ok {
				d.UnkIndex[catid] = int32(len(d.UnkContents))
			} else {
				d.UnkIndexDup[catid]++
			}
			var l, r, w int
			if l, err = strconv.Atoi(rec[jumanUnkRecordLeftIDIndex]); err != nil {
				return
			}
			if r, err = strconv.Atoi(rec[jumanUnkRecordRightIndex]); err != nil {
				return
			}
			if w, err = strconv.Atoi(rec[jumanUnkRecordWeigthIndex]); err != nil {
				return
			}
			m := dic.Morph{LeftID: int16(l), RightID: int16(r), Weight: int16(w)}
			d.UnkMorphs = append(d.UnkMorphs, m)
			d.UnkContents = append(d.UnkContents, rec[jumanUnkRecordOtherContentsStartIndex:])
		}
	}
	return
}

func loadJumanMorphFile(path string) (records [][]string, err error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = ','
	for {
		record, e := r.Read()
		if e == io.EOF {
			break
		} else if e != nil {
			err = e
			return
		}
		records = append(records, record)
	}
	return
}

func loadJumanMatrixDefFile(path string) (rowSize, colSize int64, vec []int16, err error) {
	var file *os.File
	file, err = os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	line := scanner.Text()
	dim := strings.Split(line, " ")
	if len(dim) != 2 {
		err = fmt.Errorf("invalid format: %s", line)
		return
	}
	rowSize, err = strconv.ParseInt(dim[0], 10, 0)
	if err != nil {
		err = fmt.Errorf("invalid format: %s, %s", err, line)
		return
	}
	colSize, err = strconv.ParseInt(dim[1], 10, 0)
	if err != nil {
		err = fmt.Errorf("invalid format: %s, %s", err, line)
		return
	}
	vec = make([]int16, rowSize*colSize)
	for scanner.Scan() {
		line := scanner.Text()
		ary := strings.Split(line, " ")
		if len(ary) != 3 {
			err = fmt.Errorf("invalid format: %s", line)
			return
		}
		row, e := strconv.ParseInt(ary[0], 10, 0)
		if e != nil {
			err = fmt.Errorf("invalid format: %s, %s", e, line)
			return
		}
		col, e := strconv.ParseInt(ary[1], 10, 0)
		if e != nil {
			err = fmt.Errorf("invalid format: %s, %s", e, line)
			return
		}
		val, e := strconv.Atoi(ary[2])
		if e != nil {
			err = fmt.Errorf("invalid format: %s, %s", e, line)
			return
		}
		vec[row*colSize+col] = int16(val)
	}
	if err = scanner.Err(); err != nil {
		err = fmt.Errorf("invalid format: %s, %s", err, line)
		return
	}
	return
}

func loadJumanCharClassDefFile(path string) (charClass []string, charCategory []byte, invokeMap, groupMap []bool, err error) {
	var file *os.File
	file, err = os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	charCategory = make([]byte, 65536)
	charClass = make([]string, 0)

	regCharClass := regexp.MustCompile("^(\\w+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)")
	regCharCategory := regexp.MustCompile("^(0x[0-9A-F]+)(?:\\s+([^#\\s]+))(?:\\s+([^#\\s]+))?")
	regCharCategoryRange := regexp.MustCompile("^(0x[0-9A-F]+)..(0x[0-9A-F]+)(?:\\s+([^#\\s]+))(?:\\s+([^#\\s]+))?")

	scanner := bufio.NewScanner(file)
	cc2id := make(map[string]byte)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		line = strings.TrimSpace(line)

		if matches := regCharClass.FindStringSubmatch(line); len(matches) > 0 {
			invokeMap = append(invokeMap, matches[2] == "1")
			groupMap = append(groupMap, matches[3] == "1")
			cc2id[matches[1]] = byte(len(charClass))
			charClass = append(charClass, matches[1])
		} else if matches := regCharCategory.FindStringSubmatch(line); len(matches) > 0 {
			ch, _ := strconv.ParseInt(matches[1], 0, 0)
			charCategory[ch] = cc2id[matches[2]]
		} else if matches := regCharCategoryRange.FindStringSubmatch(line); len(matches) > 0 {
			start, _ := strconv.ParseInt(matches[1], 0, 0)
			end, _ := strconv.ParseInt(matches[2], 0, 0)
			for x := start; x <= end; x++ {
				charCategory[x] = cc2id[matches[3]]
			}
		} else {
			err = fmt.Errorf("invalid format error: %v", line)
			return
		}

	}
	err = scanner.Err()
	return
}

func loadJumanUnkFile(path string) (records [][]string, err error) {
	var file *os.File
	file, err = os.Open(path)
	if err != nil {
		return
	}
	r := csv.NewReader(file)
	r.Comma = ','
	for {
		rec, e := r.Read()
		if e == io.EOF {
			break
		} else if e != nil {
			err = e
			return
		} else if len(rec) != jumanUnkRecordSize {
			err = fmt.Errorf("invalid format csv: %v, %v", file, rec)
			return
		}
		records = append(records, rec)
	}
	return
}
