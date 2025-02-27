---
icon: tools
order: 100
---

# Setup

This page focuses on all the setup required to build and run this project from scratch.
Keep in mind that this project was built on macOS. There will be some differences in commands or install steps.
Where applicable, instructions for both Unix and Windows will be provided *(to the best of my ability)*.

---

## Installs
These are programs/software that are to be installed on your computer.

### Required
1. Install Python
   * This project uses Python `v3.12.0`. Specific Python dependencies are covered in ---
     * [macOS](https://www.python.org/downloads/macos/)
       * [Windows](https://www.python.org/downloads/windows/)
2. Install Docker
   * This is used to containerize the Streamlit app.
     * [macOS](https://docs.docker.com/desktop/setup/install/mac-install/)
     * [Windows](https://docs.docker.com/desktop/setup/install/windows-install/)
3. Install editor/IDE of choice
   * To work with Python files or make your own customizations.
     * Free - [Visual Studio Code](https://code.visualstudio.com/download)
     * Free/Paid - [PyCharm](https://www.jetbrains.com/products/compare/?product=pycharm&product=pycharm-ce)

### Optional
1. Database Client
   * To view data in Motherduck although data can be viewed in Motherduck web app.
     * Pycharm has a built database connectivity model.
     * Free - [DBeaver](https://dbeaver.io/)

---

## Services

### Required
1. [Motherduck](https://motherduck.com/)
   * This is where the data for staging and prod are stored.
2. [DigitalOcean](https://www.digitalocean.com/)
   * This is where the cloud infrastructure is serviced and used.
   * DigitalOcean is covered more in depth in ---
3. [DockerHub](https://hub.docker.com/)
   * To store public Docker containers

### Optional
1. GitHub
   * To store your code changes and practice with GitHub Actions