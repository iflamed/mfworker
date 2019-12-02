package job

import "encoding/json"

type Job struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Payload []byte `json:"payload"`
}

func (j *Job) ToJson() []byte {
	res, err := json.Marshal(j)
	if err != nil {
		return nil
	}
	return res
}

func (j *Job) Unmarshal(v interface{}) error {
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
