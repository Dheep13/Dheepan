import json
import torch
import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image
import io
import os

def model_fn(model_dir):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = models.resnet50(pretrained=False)
    num_ftrs = model.fc.in_features
    model.fc = torch.nn.Linear(num_ftrs, 525)  # Adjust if your number of classes is different
    model.load_state_dict(torch.load(os.path.join(model_dir, 'model.pth'), map_location=device))
    model.eval()

    # Load class mappings
    with open(os.path.join(model_dir, 'class_mapping.json'), 'r') as f:
        class_mapping = json.load(f)

    return {'model': model, 'class_mapping': class_mapping}

def input_fn(request_body, request_content_type):
    if request_content_type == 'application/x-image':
        image = Image.open(io.BytesIO(request_body))
        transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ])
        return transform(image).unsqueeze(0)
    raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, model_dict):
    model = model_dict['model']
    class_mapping = model_dict['class_mapping']
    device = next(model.parameters()).device
    input_data = input_data.to(device)
    with torch.no_grad():
        output = model(input_data)

    _, predicted_idx = torch.max(output, 1)
    predicted_label = class_mapping['idx_to_label'][str(predicted_idx.item())]
    class_id = class_mapping['label_to_class_id'][predicted_label]
    scientific_name = class_mapping['label_to_scientific'][predicted_label]

    return {'class_id': class_id, 'scientific_name': scientific_name, 'predicted_label': predicted_label}

def output_fn(prediction, accept):
    return json.dumps(prediction)