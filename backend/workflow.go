package jarvis

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func JarvisWorkflow(ctx workflow.Context, message string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
	}
	jarvis := Start()
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, jarvis.Chat, message).Get(ctx, &result)
	if err != nil {
		return "", err
	}

	return result, nil

}
