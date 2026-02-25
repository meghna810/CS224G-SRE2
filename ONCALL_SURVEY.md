# On-Call Experience Survey for SRE/DevOps Engineers

**Purpose:** Understand on-call challenges and gauge interest in AI-powered incident investigation tools

**Target:** SREs, DevOps Engineers, Platform Engineers, Engineering Managers

---

## Survey Questions

### **Section 1: Background (2 questions)**

#### Q1. What is your current role?
**Type:** Multiple choice (select one)
- [ ] SRE / Site Reliability Engineer
- [ ] DevOps Engineer
- [ ] Platform Engineer
- [ ] Engineering Manager / Tech Lead
- [ ] Software Engineer (on-call rotation)
- [ ] Other: ___________

#### Q2. Company size (by engineering team)
**Type:** Multiple choice (select one)
- [ ] 1-10 engineers
- [ ] 11-50 engineers
- [ ] 51-200 engineers
- [ ] 201-1000 engineers
- [ ] 1000+ engineers

---

### **Section 2: Current On-Call Setup (3 questions)**

#### Q3. How does your team currently handle on-call incidents?
**Type:** Checkboxes (select all that apply)
- [ ] Manual investigation (logs, dashboards, traces)
- [ ] Runbooks and documentation
- [ ] Automated alerting (PagerDuty, Opsgenie, etc.)
- [ ] War rooms / incident channels (Slack, Teams)
- [ ] Post-mortems after incidents
- [ ] We don't have a formal on-call process
- [ ] Other: ___________

#### Q4. What incident management/alerting tools does your team use?
**Type:** Checkboxes (select all that apply)
- [ ] PagerDuty
- [ ] Opsgenie
- [ ] Rootly
- [ ] Incident.io
- [ ] VictorOps / Splunk On-Call
- [ ] Grafana OnCall
- [ ] Custom/homegrown solution
- [ ] None
- [ ] Other: ___________

#### Q5. What observability tools does your team use?
**Type:** Checkboxes (select all that apply)
- [ ] Datadog
- [ ] New Relic
- [ ] Grafana / Prometheus
- [ ] Honeycomb
- [ ] Lightstep / ServiceNow Cloud Observability
- [ ] AWS CloudWatch
- [ ] Google Cloud Monitoring
- [ ] Splunk
- [ ] Elastic / ELK Stack
- [ ] OpenTelemetry
- [ ] Other: ___________

---

### **Section 3: On-Call Pain Points (3 questions)**

#### Q6. On a scale of 1-5, how would you rate your on-call experience?
**Type:** Linear scale (1 = Very stressful, 5 = Smooth and manageable)
- 1 - Very stressful and time-consuming
- 2 - Somewhat stressful
- 3 - Neutral
- 4 - Generally manageable
- 5 - Smooth and well-organized

#### Q7. What are the BIGGEST problems you face during on-call incidents?
**Type:** Checkboxes (select up to 3)
- [ ] **Too many alerts / alert fatigue** - Hard to distinguish signal from noise
- [ ] **Don't know where to look** - Too many dashboards/tools to check
- [ ] **Permission issues** - Can't access logs, metrics, or production systems
- [ ] **Service dependencies unclear** - Don't know which service is the root cause
- [ ] **Correlating data is manual** - Hard to connect logs, traces, metrics, and code changes
- [ ] **Recent changes unknown** - Hard to know what was deployed recently
- [ ] **Long time to detect root cause** - Takes 30-60+ minutes to investigate
- [ ] **Documentation is outdated** - Runbooks don't match current architecture
- [ ] **Context switching** - Getting pulled into multiple incidents
- [ ] **Lack of domain knowledge** - On-call for services I didn't build
- [ ] Other: ___________

#### Q8. How much time do you typically spend investigating a production incident?
**Type:** Multiple choice (select one)
- [ ] Less than 15 minutes
- [ ] 15-30 minutes
- [ ] 30-60 minutes
- [ ] 1-2 hours
- [ ] 2+ hours
- [ ] Varies significantly

---

### **Section 4: Ideal Solution (2 questions)**

#### Q9. If you had a tool that could automatically investigate incidents, what would be most valuable?
**Type:** Checkboxes (rank top 3)
- [ ] **Automatic root cause identification** - "Service X failed because..."
- [ ] **Service dependency visualization** - Show impact radius
- [ ] **Correlation of logs + metrics + traces** - All in one view
- [ ] **Recent code change detection** - "This broke after commit abc123"
- [ ] **Natural language explanations** - "Database timeout caused by..."
- [ ] **Suggested remediation** - "Try reverting commit X or scaling Y"
- [ ] **Alert noise reduction** - Only notify for real issues
- [ ] **Automatic context gathering** - Pull relevant data automatically
- [ ] Other: ___________

#### Q10. Would you trust an AI agent to investigate incidents autonomously?
**Type:** Multiple choice (select one)
- [ ] Yes, if it shows its reasoning and data sources
- [ ] Yes, but only for non-critical services initially
- [ ] Maybe, would need to see it in action first
- [ ] No, prefer human-in-the-loop for all investigations
- [ ] No, AI can't handle complex production systems

---

### **Section 5: Interest in RootScout (3 questions)**

#### Q11. About RootScout
**Type:** Information only (no response needed)

> **RootScout** is an AI-powered incident investigation agent that:
> - Ingests OpenTelemetry traces, logs, and metrics
> - Monitors GitHub/GitLab for code changes
> - Builds a live service dependency graph
> - Uses LLMs (like Claude/Gemini) to identify root causes automatically
> - Provides natural language explanations: "cart-service timeout caused by DB connection pool reduced from 20→10 in commit abc123"
>
> **Time to root cause:** Seconds instead of 30-60 minutes

#### Q12. How interested would you be in trying a tool like RootScout?
**Type:** Linear scale (1 = Not interested, 5 = Very interested)
- 1 - Not interested
- 2 - Slightly interested
- 3 - Moderately interested
- 4 - Very interested
- 5 - Extremely interested - would like to try it ASAP

#### Q13. What concerns, if any, would you have about using AI for incident investigation?
**Type:** Checkboxes (select all that apply)
- [ ] **Security/Privacy** - Worried about exposing production data
- [ ] **Accuracy** - AI might give wrong answers
- [ ] **Explainability** - Need to understand how it reached conclusions
- [ ] **Integration complexity** - Too hard to set up
- [ ] **Cost** - LLM API costs might be high
- [ ] **Reliability** - What if the AI tool itself goes down?
- [ ] **Learning curve** - Team needs to learn a new tool
- [ ] **Vendor lock-in** - Don't want to depend on another vendor
- [ ] No concerns
- [ ] Other: ___________

---

### **Section 6: For Decision Makers (2 questions)**

#### Q14. Are you a founder, engineering leader, or decision-maker at your company?
**Type:** Multiple choice (select one)
- [ ] Yes - Founder/Co-founder
- [ ] Yes - VP Engineering / CTO
- [ ] Yes - Engineering Manager / Director
- [ ] No - Individual contributor
- [ ] No - Other role

#### Q15. [Only if answered "Yes" to Q14] Would your company be interested in piloting RootScout (free early access)?
**Type:** Multiple choice (select one)
- [ ] **Yes, very interested** - Please reach out to discuss
- [ ] **Maybe** - Would like to see a demo first
- [ ] **No, not at this time**
- [ ] Need more information before deciding

**If "Yes" or "Maybe", please provide:**
- Company name: ___________
- Contact email: ___________
- Company website: ___________
- Brief description of incident volume: ___________ (e.g., "5-10 incidents/week")

---

### **Section 7: Open Feedback (1 question)**

#### Q16. Any other thoughts on on-call challenges or features you'd like to see in incident investigation tools?
**Type:** Long answer (optional)
- _________________________

---

## Thank You Message

**After submission:**
> Thank you for your feedback! 🎉
>
> Your insights will help us build better incident response tools for the SRE community.
>
> **Interested in RootScout?**
> - Star our repo: https://github.com/asthamohta/CS224G-SRE
> - Connect with me on LinkedIn: [Your LinkedIn URL]
> - Email: [Your email]
>
> We'll share results and updates soon!

---

## Instructions for Creating Google Form

### Step 1: Create Form
1. Go to https://forms.google.com
2. Click "Blank form"
3. Title: "On-Call Experience Survey for SRE/DevOps Engineers"
4. Description: "Help us understand on-call challenges (5 minutes)"

### Step 2: Add Questions
Copy each question above into Google Forms with these mappings:

| Survey Type | Google Forms Type |
|-------------|-------------------|
| Multiple choice (select one) | Multiple choice |
| Checkboxes (select all) | Checkboxes |
| Linear scale | Linear scale |
| Long answer | Paragraph |
| Short answer | Short answer |

### Step 3: Configure Settings
- **Collect email addresses:** ON (for follow-up with interested founders)
- **Limit to 1 response:** ON (prevent spam)
- **Respondents can edit after submit:** OFF
- **See summary charts:** ON (you can share results)

### Step 4: Add Conditional Logic
For Q15 (pilot interest):
- Only show if Q14 = "Yes - Founder/Co-founder" OR "Yes - VP Engineering / CTO" OR "Yes - Engineering Manager / Director"
- Use Google Forms "Go to section based on answer"

### Step 5: Add Thank You Section
- Settings → Presentation → Confirmation message → [Copy text above]

---

## Distribution Tips

### LinkedIn Post:
```
🚨 Calling all SREs, DevOps Engineers, and Platform Engineers! 🚨

Quick question: How much time do you spend investigating production incidents?

I'm researching on-call challenges and building an AI-powered incident investigation tool (RootScout) that aims to reduce MTTR from 30-60 minutes to seconds.

Would love your input! 📊

👉 Survey (5 min): [Google Form Link]

Your feedback will help build better tools for our community. Plus, if you're a founder/eng leader, there's an option to pilot the tool for free!

#SRE #DevOps #OnCall #IncidentManagement #Observability
```

### Email Template:
```
Subject: Quick survey on on-call challenges? (5 min)

Hi [Name],

Hope you're doing well! I'm working on a project to improve incident investigation for SREs and DevOps teams.

Would you have 5 minutes to share your on-call experience?

Survey: [Google Form Link]

Topics covered:
• Current on-call setup
• Biggest pain points
• Interest in AI-powered investigation tools

Thanks in advance! I'll share anonymized results with everyone who participates.

Best,
[Your Name]
```

---

## What You'll Learn

From this survey, you'll get insights on:

1. **Market Size:** How many potential users in each company size
2. **Current Tools:** What you're competing against (PagerDuty, Rootly, etc.)
3. **Pain Points:** What to prioritize (alert fatigue? correlation? permissions?)
4. **Willingness to Pay:** Interest level indicates conversion potential
5. **Concerns:** What objections to address in marketing
6. **Early Customers:** Founders who want to pilot = potential design partners

---

## Success Metrics

**Target:** 50-100 responses
- At least 30% from target companies (50+ engineers)
- At least 10 "Very interested" responses
- At least 3-5 qualified pilot candidates

Good luck! 🚀
