package mfworker

import (
	"bytes"
	"encoding/json"
	"testing"
)

type TestTask struct {
	Name string
	Age int
}

func TestNewJob(t *testing.T) {
	task := &TestTask{
		Name: "Dad",
		Age: 20,
	}
	res, _ := json.Marshal(task)
	job := &Job{
		Name:    "Task",
		Payload: res,
	}
	jobStr := job.toJson()
	if jobStr == nil {
		t.Error("Serialize job failed.")
	}
	newJob := NewJobFromJSON(jobStr)
	if !bytes.Equal(newJob.Payload, job.Payload) {
		t.Errorf("The job payload not equle, expect %s but got %s.", job.Payload, newJob.Payload)
	}
	newTask := &TestTask{}
	err := newJob.Unmarshal(newTask)
	if err != nil {
		t.Errorf("Unmarshal job payload failed, get error %v", err)
	}
	if task.Name != newTask.Name || task.Age != newTask.Age {
		t.Errorf("The task is not same, expect %v but got %v", task, newTask)
	}
}