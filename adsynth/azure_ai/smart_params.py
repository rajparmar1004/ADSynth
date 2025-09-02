import os
import json
from openai import AzureOpenAI
from dotenv import load_dotenv

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
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=endpoint
        )
        self.deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "turbo-2024-04-09")
    
    def generate_parameters(self, user_prompt):
        
        system_prompt = f"""
        You are an expert Active Directory security consultant. Generate ORIGINAL ADSynth parameters.

        CRITICAL RULE: Generate completely NEW values. Do NOT copy these example numbers:
        90, 30, 35, 25, 10, 5, 15, 20, 40, 50, 4, 3, 1, 2

        ORGANIZATION PATTERNS TO FOLLOW:

        **Size Guidelines:**
        - Small (< 1000): nUsers 200-900, fewer computers, higher session percentages
        - Medium (1000-5000): nUsers 1000-5000, proportional computers  
        - Large (5000+): nUsers 5000+, lower session percentages

        **Security Level Guidelines:**
        - High Security (healthcare, finance): 
          * misconfig percentages: 1-8%
          * Modern OS emphasis (Win10/Server2016: 60-80%)
          * Low admin percentage: 5-12%
        
        - Medium Security (corporate):
          * misconfig percentages: 12-35%
          * Mixed OS distributions
          * Moderate admin percentage: 12-18%
        
        - Low Security (startup, vulnerable):
          * misconfig percentages: 40-85%
          * More legacy OS
          * Higher admin percentage: 18-28%

        **Industry Variations:**
        - Healthcare: Latest OS, very low misconfig, compliance-focused
        - Finance: Extreme security, newest systems only
        - Government: Secure but mixed systems
        - Startup: Higher misconfig, budget constraints
        - Education: Moderate security, diverse systems

        **Required JSON Structure:**
        {{
            "Domain": {{
                "functionalLevelProbability": {{
                    "2008": [0-8],
                    "2008 R2": [0-12], 
                    "2012": [8-18],
                    "2012 R2": [22-42],
                    "2016": [38-68],
                    "Unknown": [1-3]
                }}
            }},
            "Computer": {{
                "nComputers": [calculate from users],
                "enabled": [75-95],
                "haslaps": [6-28],
                "unconstraineddelegation": [6-18],
                "osProbability": {{
                    "Windows XP Professional Service Pack 3": [0-12],
                    "Windows 7 Professional Service Pack 1": [0-22],
                    "Windows 7 Ultimate Service Pack 1": [0-18],
                    "Windows 7 Enterprise Service Pack 1": [8-28],
                    "Windows 10 Pro": [22-48],
                    "Windows 10 Enterprise": [24-58]
                }},
                "privesc": [18-65],
                "creddump": [22-68],
                "exploitable": [18-68],
                "computerProbability": {{
                    "PAW": [12-28],
                    "Server": [16-32],
                    "Workstation": [42-72]
                }}
            }},
            "DC": {{
                "enabled": [82-96],
                "haslaps": [6-28],
                "osProbability": {{
                    "Windows Server 2003 Enterprise Edition": [0-3],
                    "Windows Server 2008 Standard": [0-3],
                    "Windows Server 2008 Datacenter": [0-3],
                    "Windows Server 2008 Enterprise": [0-3],
                    "Windows Server 2008 R2 Standard": [1-6],
                    "Windows Server 2008 R2 Datacenter": [2-8],
                    "Windows Server 2008 R2 Enterprise": [2-8],
                    "Windows Server 2012 Standard": [3-8],
                    "Windows Server 2012 Datacenter": [3-8],
                    "Windows Server 2012 R2 Standard": [8-18],
                    "Windows Server 2012 R2 Datacenter": [8-18],
                    "Windows Server 2016 Standard": [28-48],
                    "Windows Server 2016 Datacenter": [18-38]
                }}
            }},
            "User": {{
                "nUsers": [based on org size],
                "enabled": [92-98],
                "dontreqpreauth": [3-12],
                "hasspn": [6-18],
                "passwordnotreqd": [2-12],
                "pwdneverexpires": [42-68],
                "sidhistory": [6-18],
                "unconstraineddelegation": [16-28],
                "savedcredentials": [32-52],
                "Kerberoastable": [[2-6], [4-8]],
                "sessionsPercentages": [adjust by size],
                "priority_session_weight": [1],
                "perc_special_roles": [6-18]
            }},
            [Continue with all sections using ORIGINAL values in the given ranges]
        }}

        Generate for: {user_prompt}
        
        Return ONLY the complete JSON with ORIGINAL values.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generate unique ADSynth parameters for: {user_prompt}"}
                ],
                temperature=0.8,
                max_tokens=3000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Extract JSON from response
            start = content.find('{')
            end = content.rfind('}') + 1
            
            if start == -1 or end == 0:
                print("No valid JSON found in response")
                return None
                
            json_str = content[start:end]
            generated_params = json.loads(json_str)
            
            # Basic validation only
            if (generated_params.get("User", {}).get("nUsers", 0) > 0 and 
                generated_params.get("Computer", {}).get("nComputers", 0) > 0):
                return generated_params
            else:
                print("Generated parameters validation failed - missing critical values")
                return None
            
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            return None
        except Exception as e:
            print(f"Azure AI error: {e}")
            return None