package cloudevents

import (
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
	"github.com/cloudevents/sdk-go/v2/types"
)

// IDExtractorTransformer implements binding.Transformer. Upon the execution of the transformer, the underlying value
// if updated to be event ID.
type IDExtractorTransformer string

func (a *IDExtractorTransformer) Transform(reader binding.MessageMetadataReader, _ binding.MessageMetadataWriter) error {
	_, ty := reader.GetAttribute(spec.ID)
	if ty != nil {
		tyParsed, err := types.ToString(ty)
		if err != nil {
			return err
		}
		*a = IDExtractorTransformer(tyParsed)
	}

	return nil
}
