import torch
import torchvision.models as models

def test_model_structure():
    model = models.resnet50(pretrained=False)
    model.fc = torch.nn.Linear(model.fc.in_features, 525)  # Assuming 525 bird classes
    assert isinstance(model.fc, torch.nn.Linear), "Final layer should be Linear"
    assert model.fc.out_features == 525, "Output features should match number of classes"

def test_model_output():
    model = models.resnet50(pretrained=False)
    model.fc = torch.nn.Linear(model.fc.in_features, 525)
    input_tensor = torch.randn(1, 3, 224, 224)
    output = model(input_tensor)
    assert output.shape == (1, 525), "Output shape should be (1, 525)"