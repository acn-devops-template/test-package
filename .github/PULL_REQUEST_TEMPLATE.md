## Pull Request Guideline

Please, go through these steps before you submit a PR.

1. Make sure that your PR is not a duplicate.
2. If not, then make sure that:

   - You have done your changes in a separate branch. Branches MUST have descriptive names that start with prefix such as the `fix/` and `feature/`. Good examples are: `fix/signin-issue` or `feature/issue-templates`.

   - You have a descriptive commit message with a short title (first line).

   - You have only one commit (if not, squash them into one commit).

   - `npm test` doesn't throw any error. If it does, fix them first and amend your commit (`git commit --amend`).

3. **After** these steps, you're ready to open a pull request.

   - Your pull request **MUST NOT** target the `master` branch on this repository.

   - Give a descriptive title to your PR.

   - Describe your changes.

   - Put `closes #XXXX` in your comment to auto-close the issue that your PR fixes (if such).

IMPORTANT: Please review the [CONTRIBUTING.md](../CONTRIBUTING.md) file for detailed contributing guidelines.

**PLEASE REMOVE THIS LINE AND ALL ABOVE BEFORE SUBMITTING !!**

# Ticket

Please provide a link to the ticket related to this change

# Description

Please include a summary of the change and which issue is fixed. Please also include relevant motivation and context. List any dependencies that are required for this change.

## Type of change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] This change requires a documentation update

## Demonstration (if any)

##### description topic 1

screenshot or image (if appropriate) with description here

##### description topic 2

screenshot or image (if appropriate) with description here

# How Has This Been Tested?

Please describe the tests that you ran to verify your changes. Provide instructions so we can reproduce. Please also list any relevant details for your test configuration

- [ ] Test A
- [ ] Test B

# Checklist:

- [ ] My code follows the style guidelines of this project
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes
- [ ] Any dependent changes have been merged and published in downstream modules
