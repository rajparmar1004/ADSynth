import os
import json
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

class SmartParameterGenerator:
    def __init__(self):
        # Load .env file from the root directory
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(env_path)
        
        # Debug: Print if variables are loaded
        api_key = os.getenv("AZURE_OPENAI_API_KEY")
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        
        if not api_key:
            print("AZURE_OPENAI_API_KEY not found in environment")
            raise ValueError("Missing Azure OpenAI API key")
        
        if not endpoint:
            print("AZURE_OPENAI_ENDPOINT not found in environment")
            raise ValueError("Missing Azure OpenAI endpoint")
        
        print(f"API Key loaded: {api_key[:10]}...")
        print(f"Endpoint loaded: {endpoint}")
        
        self.client = AzureOpenAI(
            api_key=api_key,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2025-04-01-preview"),
            azure_endpoint=endpoint
        )
        self.deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "turbo-2024-04-09")
    
    def generate_parameters(self, user_prompt):
        system_prompt = """
        You are an expert at converting organizational descriptions to ADSynth JSON parameters.
        
        Organization Size Guidelines:
        - Small (startup, clinic): 100-500 users, 150-300 computers
        - Medium (department, branch): 500-2000 users, 800-1600 computers  
        - Large (enterprise): 2000+ users, 3000+ computers
        
        Security Levels:
        - Low: High misconfig percentages (30-60%), more vulnerabilities
        - High: Low misconfig percentages (0-5%), better security
        
        Industry Examples:
        - Healthcare: High security, compliance focused
        - Finance: Maximum security, audit requirements
        - Startup: Lower security, rapid growth
        
        Return ONLY valid JSON matching ADSynth schema. Focus on:
        - User.nUsers and Computer.nComputers
        - perc_misconfig_sessions and perc_misconfig_permissions
        - Admin.Admin_Percentage
        - Security-related percentages
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generate ADSynth parameters for: {user_prompt}"}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content
            # Extract JSON from response
            start = content.find('{')
            end = content.rfind('}') + 1
            json_str = content[start:end]
            
            return json.loads(json_str)
            
        except Exception as e:
            print(f"Azure AI error: {e}")
            return None
