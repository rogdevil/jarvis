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

func ProcessChatMessageWorkflow(ctx workflow.Context) (int64, error) {

	processMessageSignalCh := workflow.GetSignalChannel(ctx, ProcessMessageSignal)

	for {
		var message string
		processMessageSignalCh.Receive(ctx, &message)

		activityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute * 1,
			TaskQueue:           "process-message-queue",
		}

		ctx = workflow.WithActivityOptions(ctx, activityOptions)

		var res int64
		err := workflow.ExecuteActivity(ctx, "GetChatLengthActivity", message).Get(ctx, &res)

		if err != nil {
			workflow.GetLogger(ctx).Error("Activity execution failed", "error", err)
		} else {
			workflow.GetLogger(ctx).Info("Activity executed successfully", res)
		}

		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 50_000 {
			break
		}
	}

	return 0, workflow.NewContinueAsNewError(ctx, ProcessChatMessageWorkflow)
}
