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
    
    def validate_os_probabilities(self, os_dict):
        """Ensure OS probabilities sum to exactly 100%"""
        total = sum(os_dict.values())
        if abs(total - 100.0) > 0.1:  # Allow 0.1% tolerance
            # Proportionally adjust all values
            for key in os_dict:
                os_dict[key] = round((os_dict[key] * 100.0 / total), 1)
        return os_dict
    
    def validate_and_fix_parameters(self, params):
        """Apply validation and fixes to generated parameters"""
        try:
            # Fix Domain Controller OS probabilities
            if "DC" in params and "osProbability" in params["DC"]:
                params["DC"]["osProbability"] = self.validate_os_probabilities(params["DC"]["osProbability"])
            
            # Fix Computer OS probabilities
            if "Computer" in params and "osProbability" in params["Computer"]:
                params["Computer"]["osProbability"] = self.validate_os_probabilities(params["Computer"]["osProbability"])
            
            # Fix Domain functional level probabilities
            if "Domain" in params and "functionalLevelProbability" in params["Domain"]:
                params["Domain"]["functionalLevelProbability"] = self.validate_os_probabilities(params["Domain"]["functionalLevelProbability"])
            
            # Ensure session percentages follow scaling rules
            n_users = params.get("User", {}).get("nUsers", 0)
            if n_users > 0:
                if n_users <= 200:
                    correct_sessions = [2.0, 2.0, 2.0]
                elif n_users <= 500:
                    correct_sessions = [1.5, 1.5, 1.5]
                elif n_users <= 1000:
                    correct_sessions = [1.1, 1.1, 1.1]
                elif n_users <= 2000:
                    correct_sessions = [0.9, 0.9, 0.9]
                elif n_users <= 5000:
                    correct_sessions = [0.6, 0.6, 0.6]
                else:
                    correct_sessions = [0.21, 0.21, 0.21]
                
                if "User" in params:
                    params["User"]["sessionsPercentages"] = correct_sessions
            
            # Clean up misconfig arrays (ensure single values, not arrays)
            misconfig_keys = [
                "perc_misconfig_sessions", "perc_misconfig_permissions",
                "perc_misconfig_permissions_on_groups", "perc_misconfig_nesting_groups"
            ]
            for key in misconfig_keys:
                if key in params:
                    for subkey in ["Customized", "Low", "High"]:
                        if subkey in params[key] and isinstance(params[key][subkey], list):
                            params[key][subkey] = params[key][subkey][0] if params[key][subkey] else 0
            
            # Add ACLPrivilegedPercentage if missing
            if "ACLs" in params and "ACLPrivilegedPercentage" not in params["ACLs"]:
                params["ACLs"]["ACLPrivilegedPercentage"] = 5
            
            # CRITICAL: Round all Azure count fields to integers (AI sometimes generates decimals)
            azure_int_fields = [
                ("AZUser", "nUsers"),
                ("AZGroup", "nGroups"),
                ("AZServicePrincipal", "nServicePrincipals"),
                ("AZApp", "nApplications"),
                ("AZKeyVault", "nKeyVaults"),
                ("AZVM", "nVMs"),
                ("AZManagementGroup", "nManagementGroups"),
                ("AZSubscription", "nSubscriptions")
            ]
            
            for section, field in azure_int_fields:
                if section in params and field in params[section]:
                    value = params[section][field]
                    if isinstance(value, (int, float)):
                        params[section][field] = max(1, int(round(value)))  # Ensure at least 1
            
            # Ensure Azure parameters have proper structure (but don't override AI values)
            if "AZUser" not in params:
                print("Warning: No Azure parameters generated, adding minimal defaults")
                params["AZTenant"] = {"nTenants": 1}
                params["AZSubscription"] = {"nSubscriptions": 1}
                params["AZUser"] = {"nUsers": 50, "enabled": 90}
                params["AZGroup"] = {"nGroups": 10, "nDefaultGroups": 2, "nMembersPerGroup": [1, 10]}
                params["AZServicePrincipal"] = {"nServicePrincipals": 5}
                params["AZApp"] = {"nApplications": 5, "nDefaultApplications": 1, "spAssignmentProbability": 80}
                params["AZRole"] = {
                    "nRoles": 3,
                    "defaultRoles": ["Global Administrator", "Contributor", "Reader"],
                    "assignChanceUsers": 25,
                    "assignChanceGroups": 30,
                    "assignChanceServicePrincipals": 100
                }
                params["AZManagementGroup"] = {"nManagementGroups": 2, "subscriptionsPerGroup": [1, 3]}
                params["AZKeyVault"] = {"nKeyVaults": 3, "accessPolicyProbability": 20}
                params["AZVM"] = {"nVMs": 5, "enabled": 90}
                params["AZMisconfig"] = {
                    "overprivileged_users": 10,
                    "misconfig_group_members": 5,
                    "reset_password": 5,
                    "add_member": 5,
                    "add_secret": 5,
                    "owns_resource": 10
                }
            
            return params
            
        except Exception as e:
            print(f"Validation error: {e}")
            return params
    
    def generate_parameters(self, user_prompt):
        
        system_prompt = """
You are an expert Active Directory and Azure/Entra ID security consultant. Generate COMPLETE ADSynth parameters for HYBRID environments (on-premises + Azure AD/Entra ID).

CRITICAL REQUIREMENTS:
1. **EXACT USER COUNT MATCHING**: Match user specification EXACTLY for both on-prem and Azure
2. **OS PROBABILITY VALIDATION**: ALL OS dictionaries MUST sum to 100%
3. **SECURITY LEVEL ACCURACY**: Misconfig % must reflect security posture across BOTH environments
4. **HYBRID INTELLIGENCE**: Azure parameters should reflect cloud adoption strategy and security maturity

**HYBRID DEPLOYMENT PATTERNS:**

CLOUD-FIRST (Modern, SaaS-heavy):
- Azure users: 80-100% of on-prem users
- High Azure role assignments (30-40% users with roles)
- More service principals (0.1-0.15 per user)
- Higher misconfig risk (5-10% overprivileged users)
- More VMs and Key Vaults

HYBRID BALANCED (Traditional enterprise):
- Azure users: 30-60% of on-prem users
- Moderate role assignments (20-30%)
- Standard service principals (0.05-0.08 per user)
- Medium misconfig (3-7% overprivileged)

ON-PREM FOCUSED (Legacy, regulated):
- Azure users: 10-25% of on-prem users
- Conservative role assignments (15-25%)
- Minimal service principals (0.03-0.05 per user)
- Lower cloud misconfig (1-5%)

**ORGANIZATION SIZE & SCALING:**
- Small (50-500): Computer ratio 0.8-1.0, sessions [2.0, 2.0, 2.0]
- Medium (500-2000): Computer ratio 0.9-1.1, sessions [1.1, 1.1, 1.1]
- Large (2000-10000): Computer ratio 1.0-1.2, sessions [0.6, 0.6, 0.6]
- Enterprise (10000+): Computer ratio 1.1-1.3, sessions [0.21, 0.21, 0.21]

**SECURITY PATTERNS:**

HIGH SECURITY (Healthcare, Finance, Government):
ON-PREM:
- Windows 10 Enterprise: 65-85%
- Server 2016 combined: 70-85%
- LAPS: 40-50%
- Misconfig sessions: 0.01-0.1%
- Misconfig permissions: 0.01-0.1%
- Exploitable: 5-15%

AZURE:
- Overprivileged users: 1-3%
- Misconfig group members: 1-2%
- Reset password: 1-2%
- Add member/secret: 1-3%
- Owns resource: 2-5%
- Higher Key Vault usage (0.05-0.08 per user)

MEDIUM SECURITY (Standard Enterprise):
ON-PREM:
- Windows 10 Enterprise: 40-60%
- Server 2016 combined: 50-70%
- LAPS: 25-40%
- Misconfig sessions: 1-5%
- Misconfig permissions: 1-5%
- Exploitable: 20-35%

AZURE:
- Overprivileged users: 5-8%
- Misconfig group members: 3-5%
- Reset password: 3-5%
- Add member/secret: 3-5%
- Owns resource: 5-10%
- Standard Key Vault usage (0.03-0.05 per user)

LOW SECURITY (Small Business, Budget):
ON-PREM:
- Windows 10 Enterprise: 20-40%
- Server 2016 combined: 35-55%
- LAPS: 10-25%
- Misconfig sessions: 10-30%
- Misconfig permissions: 10-30%
- Exploitable: 35-50%

AZURE:
- Overprivileged users: 10-20%
- Misconfig group members: 5-10%
- Reset password: 5-10%
- Add member/secret: 5-10%
- Owns resource: 10-20%
- Minimal Key Vault usage (0.01-0.03 per user)

**AZURE SCALING GUIDELINES:**
- Groups: 0.1-0.2 per user (more for large orgs)
- Service Principals: 0.03-0.15 per user (based on automation maturity)
- Applications: Same as service principals
- VMs: 0.05-0.15 per user (based on cloud adoption)
- Key Vaults: 0.01-0.08 per user (based on security maturity)
- Management Groups: 1-5 (based on org complexity)

Generate COMPLETE JSON with ALL sections below. Return ONLY valid JSON, no explanations.

Required format:
{
    "Domain": {
        "functionalLevelProbability": {
            "2008": <number>,
            "2008 R2": <number>,
            "2012": <number>,
            "2012 R2": <number>,
            "2016": <number>,
            "Unknown": <number>
        }
    },
    "Computer": {
        "nComputers": <exact_number>,
        "enabled": <80-95>,
        "haslaps": <10-50>,
        "unconstraineddelegation": <5-15>,
        "osProbability": {
            "Windows XP Professional Service Pack 3": <0-5>,
            "Windows 7 Professional Service Pack 1": <2-15>,
            "Windows 7 Ultimate Service Pack 1": <1-10>,
            "Windows 7 Enterprise Service Pack 1": <5-25>,
            "Windows 10 Pro": <15-35>,
            "Windows 10 Enterprise": <20-85>
        },
        "privesc": <10-50>,
        "creddump": <10-50>,
        "exploitable": <10-50>,
        "computerProbability": {
            "PAW": 20,
            "Server": 30,
            "Workstation": 50
        }
    },
    "DC": {
        "enabled": <85-95>,
        "haslaps": <10-50>,
        "osProbability": {
            "Windows Server 2003 Enterprise Edition": <0-2>,
            "Windows Server 2008 Standard": <0-3>,
            "Windows Server 2008 Datacenter": <0-3>,
            "Windows Server 2008 Enterprise": <0-3>,
            "Windows Server 2008 R2 Standard": <1-5>,
            "Windows Server 2008 R2 Datacenter": <1-6>,
            "Windows Server 2008 R2 Enterprise": <1-6>,
            "Windows Server 2012 Standard": <2-8>,
            "Windows Server 2012 Datacenter": <2-8>,
            "Windows Server 2012 R2 Standard": <4-15>,
            "Windows Server 2012 R2 Datacenter": <4-15>,
            "Windows Server 2016 Standard": <25-45>,
            "Windows Server 2016 Datacenter": <25-45>
        }
    },
    "User": {
        "nUsers": <exact_match_to_request>,
        "enabled": <90-98>,
        "dontreqpreauth": <1-10>,
        "hasspn": <3-15>,
        "passwordnotreqd": <1-10>,
        "pwdneverexpires": <5-60>,
        "sidhistory": <1-15>,
        "unconstraineddelegation": <1-25>,
        "savedcredentials": <5-50>,
        "Kerberoastable": [1, 5],
        "sessionsPercentages": [<use_scaling_pattern>],
        "priority_session_weight": 1,
        "perc_special_roles": 10
    },
    "Group": {
        "nestingGroupProbability": <5-35>,
        "departmentProbability": {
            "IT": 25,
            "R&D": 25,
            "BUSINESS": 25,
            "HR": 25
        },
        "nResourcesThresholds": [<based_on_size>],
        "nLocalAdminsPerDepartment": [3, 5],
        "nOUsPerLocalAdmins": [3, 5],
        "nGroupsPerUsers": [3, 5]
    },
    "GPO": {
        "nGPOs": <30-50>,
        "exploitable": <10-40>
    },
    "ACLs": {
        "ACLPrincipalsPercentage": <10-35>,
        "ACLPrivilegedPercentage": 5,
        "ACLsProbability": {
            "GenericAll": <1-15>,
            "GenericWrite": <1-20>,
            "WriteOwner": <1-20>,
            "WriteDacl": <1-20>,
            "AddMember": <1-35>,
            "ForceChangePassword": <1-20>,
            "AllExtendedRights": <1-15>
        }
    },
    "nonACLs": {
        "nonACLsPercentage": <5-20>,
        "nonACLsProbability": {
            "CanRDP": 25,
            "ExecuteDCOM": 25,
            "AllowedToDelegate": 25,
            "ReadLAPSPassword": 25
        }
    },
    "perc_misconfig_sessions": {
        "Low": <based_on_security>,
        "High": <based_on_security>,
        "Customized": <based_on_security>
    },
    "perc_misconfig_permissions": {
        "Low": <based_on_security>,
        "High": <based_on_security>,
        "Customized": <based_on_security>
    },
    "perc_misconfig_permissions_on_groups": {
        "Low": 0,
        "High": 100,
        "Customized": 0
    },
    "perc_misconfig_nesting_groups": {
        "Low": 0,
        "High": 20,
        "Customized": 0
    },
    "misconfig_permissions_to_tier_0": {
        "allow": 1,
        "limit": 1
    },
    "misconfig_group": {
        "acl_ratio": <10-60>,
        "admin_ratio": <10-40>,
        "priority_paws_weight": 3
    },
    "nTiers": 3,
    "Tier_1_Servers": {
        "extraServers": []
    },
    "Admin": {
        "service_account": <5-20>,
        "Admin_Percentage": <10-25>
    },
    "nodeMisconfig": {
        "admin_regular": 0,
        "user_comp": 0
    },
    "nLocations": 3,
    "convert_to_directed_graphs": 0,
    "seed": 1,
    
    "AZTenant": {
        "nTenants": 1
    },
    "AZSubscription": {
        "nSubscriptions": <1-5_based_on_org_size>
    },
    "AZUser": {
        "nUsers": <based_on_hybrid_pattern_and_on_prem_users>,
        "enabled": <85-98>
    },
    "AZGroup": {
        "nGroups": <0.1-0.2_times_azure_users>,
        "nDefaultGroups": 2,
        "nMembersPerGroup": [<min>, <max_based_on_users>]
    },
    "AZServicePrincipal": {
        "nServicePrincipals": <0.03-0.15_times_azure_users>
    },
    "AZApp": {
        "nApplications": <same_as_service_principals>,
        "nDefaultApplications": 1,
        "spAssignmentProbability": <70-90>
    },
    "AZRole": {
        "nRoles": 3,
        "defaultRoles": ["Global Administrator", "Contributor", "Reader"],
        "assignChanceUsers": <15-40_based_on_maturity>,
        "assignChanceGroups": <20-40>,
        "assignChanceServicePrincipals": 100
    },
    "AZManagementGroup": {
        "nManagementGroups": <1-5_based_on_complexity>,
        "subscriptionsPerGroup": [1, <2-5>]
    },
    "AZKeyVault": {
        "nKeyVaults": <0.01-0.08_times_azure_users>,
        "accessPolicyProbability": <15-30>
    },
    "AZVM": {
        "nVMs": <0.05-0.15_times_azure_users>,
        "enabled": <85-95>
    },
    "AZMisconfig": {
        "overprivileged_users": <1-20_based_on_security>,
        "misconfig_group_members": <1-10>,
        "reset_password": <1-10>,
        "add_member": <1-10>,
        "add_secret": <1-10>,
        "owns_resource": <2-20>
    }
}

CRITICAL: Generate Azure parameters that reflect the organization's cloud strategy, security posture, and maturity level described in the prompt.
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Generate complete ADSynth parameters for: {user_prompt}"}
                ],
                temperature=0.7,
                max_tokens=4000
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
            
            # Apply validation and fixes
            generated_params = self.validate_and_fix_parameters(generated_params)
            
            # Validate that all required sections are present
            required_sections = [
                "Domain", "Computer", "DC", "User", "Group", "GPO", "ACLs",
                "perc_misconfig_sessions", "perc_misconfig_permissions", 
                "perc_misconfig_permissions_on_groups", "perc_misconfig_nesting_groups",
                "misconfig_permissions_to_tier_0", "misconfig_group", "nTiers",
                "Tier_1_Servers", "Admin", "nonACLs", "nodeMisconfig", 
                "nLocations", "convert_to_directed_graphs", "seed",
                # Azure sections
                "AZTenant", "AZSubscription", "AZUser", "AZGroup", 
                "AZServicePrincipal", "AZApp", "AZRole", "AZManagementGroup",
                "AZKeyVault", "AZVM", "AZMisconfig"
            ]
            
            missing_sections = [section for section in required_sections if section not in generated_params]
            
            if missing_sections:
                print(f"Warning: Missing sections: {missing_sections}")
            
            # Validate critical parameters
            n_users = generated_params.get("User", {}).get("nUsers", 0)
            n_computers = generated_params.get("Computer", {}).get("nComputers", 0)
            n_azure_users = generated_params.get("AZUser", {}).get("nUsers", 0)
            
            # Ensure values are integers, not lists
            if isinstance(n_users, list):
                n_users = n_users[0] if n_users else 0
                generated_params["User"]["nUsers"] = n_users
            if isinstance(n_computers, list):
                n_computers = n_computers[0] if n_computers else 0
                generated_params["Computer"]["nComputers"] = n_computers
            if isinstance(n_azure_users, list):
                n_azure_users = n_azure_users[0] if n_azure_users else 0
                generated_params["AZUser"]["nUsers"] = n_azure_users
                
            if (n_users > 0 and n_computers > 0):
                print(f"\n✓ Generated parameters successfully:")
                print(f"  On-Prem: {n_users} users, {n_computers} computers")
                print(f"  Azure: {n_azure_users} users ({round(n_azure_users/n_users*100)}% of on-prem)")
                print(f"  Service Principals: {generated_params.get('AZServicePrincipal', {}).get('nServicePrincipals', 0)}")
                print(f"  VMs: {generated_params.get('AZVM', {}).get('nVMs', 0)}")
                print(f"  Key Vaults: {generated_params.get('AZKeyVault', {}).get('nKeyVaults', 0)}")
                return generated_params
            else:
                print("Generated parameters validation failed - missing critical values")
                return None
            
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            print(f"Raw response: {content[:500]}...")
            return None
        except Exception as e:
            print(f"Azure AI error: {e}")
            import traceback
            traceback.print_exc()
            return None