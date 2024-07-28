package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bunyawats/simple-go-htmx/data"
	"github.com/russross/blackfriday/v2"
	goopenai "github.com/sashabaranov/go-openai"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
	"log"
	"slices"
	"time"
)

// function name
const getCustomerAge = "get_customer_age"

// parameter name
const firstName = "firstName"

type (
	OpenAIService struct {
		*openai.LLM
		*goopenai.Client
		goopenai.Assistant
		data.Repository
	}
)

func NewOpenAIService(llm *openai.LLM, client *goopenai.Client, assistant goopenai.Assistant, repo *data.Repository) *OpenAIService {
	return &OpenAIService{
		LLM:        llm,
		Client:     client,
		Assistant:  assistant,
		Repository: *repo,
	}
}

func (s *OpenAIService) UseLLM(msg string) (string, error) {

	log.Println("useLLM")

	content := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, "You are a company branding design wizard."),
		llms.TextParts(llms.ChatMessageTypeHuman, msg),
	}
	ctx := context.Background()

	res, err := s.LLM.GenerateContent(ctx, content,
		llms.WithMaxTokens(1024),
		llms.WithStreamingFunc(func(ctx context.Context, chunk []byte) error {
			fmt.Print(string(chunk))
			return nil
		}),
	)
	if err != nil {
		fmt.Printf("Completion error: %v\n", err)
		return "", err
	}
	return res.Choices[0].Content, err
}

func (s *OpenAIService) UseOpenAI(msg string) (string, error) {

	log.Println("useOpenAI")

	resp, err := s.CreateChatCompletion(
		context.Background(),
		goopenai.ChatCompletionRequest{
			Model:     goopenai.GPT4o,
			MaxTokens: 100,
			Messages: []goopenai.ChatCompletionMessage{
				{
					Content: msg,
					Role:    "user",
				},
			},
		},
	)
	if err != nil {
		fmt.Printf("Completion error: %v\n", err)
		return "", err
	}
	return resp.Choices[0].Message.Content, err
}

func (s *OpenAIService) UseOpenAIAssistant(msg string) (string, error) {

	log.Println("useOpenAIAssistant")

	ctx := context.Background()

	thread, err := s.CreateThread(
		ctx,
		goopenai.ThreadRequest{
			Messages: []goopenai.ThreadMessage{
				{
					Role:    goopenai.ThreadMessageRoleUser,
					Content: "Hello AI",
				},
			},
		},
	)
	if err != nil {
		fmt.Printf("Create thred error: %v\n", err)
		return "", err
	}
	log.Println("Thread ID", thread.ID)

	message, err := s.CreateMessage(
		ctx,
		thread.ID,
		goopenai.MessageRequest{
			Role:    string(goopenai.ThreadMessageRoleUser),
			Content: msg,
		},
	)
	if err != nil {
		fmt.Printf("Create thred message error: %v\n", err)
		return "", err
	}
	log.Println("Thread Message ID", message.ID)

	threadRun, err := s.CreateRun(
		ctx,
		thread.ID,
		goopenai.RunRequest{
			AssistantID: s.Assistant.ID,
			Model:       goopenai.GPT4o,
		},
	)
	if err != nil {
		fmt.Printf("Run thread error: %v\n", err)
		return "", err
	}
	log.Println("Thread Run ID", threadRun.ID)

	for {
		threadRun, err = s.RetrieveRun(ctx, thread.ID, threadRun.ID)
		if err != nil {
			fmt.Printf("Retrive Run thread error: %v\n", err)
			return "", err
		}

		fmt.Println(threadRun.Status, ".")
		if goopenai.RunStatusCompleted == threadRun.Status {
			limit := 1
			msgList, err := s.ListMessage(ctx, thread.ID, &limit, nil, nil, nil)
			if err != nil {
				fmt.Printf("List thread msgs error: %v\n", err)
				return err.Error(), err
			}

			for _, msg := range msgList.Messages {
				if len(msg.Content) > 0 && msg.Role != "user" {
					log.Println(msg.Content[0].Text.Value)
					html := blackfriday.Run([]byte(msg.Content[0].Text.Value))
					return string(html), nil
				}
			}
			break
		}
		if slices.Contains([]goopenai.RunStatus{
			goopenai.RunStatusCancelled,
			goopenai.RunStatusExpired,
			goopenai.RunStatusFailed},
			threadRun.Status,
		) {
			errMsg := threadRun.LastError.Message
			return errMsg, nil
		}
		if goopenai.RunStatusRequiresAction == threadRun.Status {
			fmt.Println("RequiresAction", "Submit tool output")

			tooCall, funcOutput, err := s.useExternalFunction(threadRun.RequiredAction.SubmitToolOutputs)
			if err != nil {
				return "", err
			}
			log.Println("funcOutput", funcOutput)

			threadRun, err = s.SubmitToolOutputs(ctx, thread.ID, threadRun.ID,
				goopenai.SubmitToolOutputsRequest{
					ToolOutputs: []goopenai.ToolOutput{
						{
							ToolCallID: tooCall.ID,
							Output:     funcOutput,
						},
					},
				},
			)
			if err != nil {
				fmt.Printf("Submit Tool Outputs error: %v\n", err)
				return "", err
			}
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", nil
}

func (s *OpenAIService) useExternalFunction(outputs *goopenai.SubmitToolOutputs) (goopenai.ToolCall, string, error) {

	for _, toolCall := range outputs.ToolCalls {

		parametersJson := toolCall.Function.Arguments
		functionName := toolCall.Function.Name
		log.Println("functionName", functionName)
		log.Println("parametersJson", parametersJson)

		switch functionName {
		case getCustomerAge:
			{
				funcOutput, err := s.getCustomerAge(parametersJson)
				return toolCall, funcOutput, err
			}

		}

	}
	return goopenai.ToolCall{}, "nil", errors.New("external function not supported")

}

func (s *OpenAIService) getCustomerAge(parametersJson string) (string, error) {

	var parametersMap map[string]string
	err := json.Unmarshal([]byte(parametersJson), &parametersMap)
	if err != nil {
		log.Printf("Error unmarshalling JSON: %v", err)
		return "", err
	}

	firstName := parametersMap[firstName]
	log.Println("GetAgeByFirstName firstName", firstName)

	age, err := s.GetAgeByFirstName(firstName)
	if err != nil {
		log.Printf("Error GetAgeByFirstName from Database: %v", err)
		return `NA`, nil
	}
	return fmt.Sprintf(`{ age: %v }`, age), nil
}
