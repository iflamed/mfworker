package mfworker

import "encoding/json"

type Job struct {
	Name string `json:"name"`
	Payload []byte `json:"payload"`
}

func (j *Job) toJson() []byte  {
	res, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return res
}

func (j *Job) Unmarshal(v interface{}) error  {
	return json.Unmarshal(j.Payload, v)
}

func NewJobFromJSON(str []byte) *Job {
	j := &Job{}
	err := json.Unmarshal(str, j)
	if err != nil {
		return nil
	}
	return j
}