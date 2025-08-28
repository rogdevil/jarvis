package jarvis

import (
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

func JarvisWorkflow(ctx workflow.Context, message string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
	}
	cwo := workflow.ChildWorkflowOptions{
		ParentClosePolicy:   enums.PARENT_CLOSE_POLICY_ABANDON,
		WaitForCancellation: false,
	}

	jarvis := Start()
	ctx = workflow.WithActivityOptions(ctx, ao)
	childWorkflowContext := workflow.WithChildOptions(ctx, cwo)

	fmt.Println("started child workflow")
	workflow.ExecuteChildWorkflow(childWorkflowContext, ProcessChatMessageWorkflow, message)
	fmt.Println("child workflow is abandoned")

	var result string
	err := workflow.ExecuteActivity(ctx, jarvis.Chat, message).Get(ctx, &result)
	if err != nil {
		return "", err
	}

	return result, nil

}

func ProcessChatMessageWorkflow(ctx workflow.Context, message string) (int64, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 1,
	}
	jarvis := Start()
	ctx = workflow.WithActivityOptions(ctx, ao)

	var length int64
	err := workflow.ExecuteActivity(ctx, jarvis.GetChatLengthActivity, message).Get(ctx, &length)

	if err != nil {
		return 0, err
	}

	return length, nil
}
