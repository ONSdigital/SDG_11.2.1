# Using mkdocs to edit these pages


## Modifying a page

If you want to edit an existing page, you need to edit the corresponding `.md` file which can be found in the `docs/` folder of this project. 

Follow these instructions:

1. Create a new branch (with the example name of `my-page-edits`) using Git with the following command:

`git checkout -b my-page-edits`

2. Make the necessary changes to the markdown files that you want to update. Git add and commit as usual. 

3. After committing your changes, run the following command to rebuild your site:

`mkdocs build`

4. After running the build command, mkdocs will create a new set of HTML files in the 'site/' directory, replacing the previous version of your site.

5. Before pushing, it's a good idea to see the changes on your local machine. To preview your changes, you can run the following command to launch a local server:

`mkdocs serve`

 Open your web browser and navigate to http://localhost:8000 to see your changes in action.

6. If you are happy with your changes, you can push your updated branch to your Git repository. 

7. After that create a pull request to `main` and the Github Action (controlled by this yaml `.github/workflows/pages.yml`) should trigger the re-building and deployment of the pages. 


## Manually deploying pages with `gh-deploy`

You can deploy any changes you have made locally using `mkdocs gh-deploy`.

Look for more guidance in the [mkdocs official documentation](https://www.mkdocs.org/user-guide/deploying-your-docs/)

## Repository "Pages" settings 

You should select the "gh-pages" branch as the source branch in the Pages section of Github's Pages settings.

Here are the steps to follow:

    Go to your Github repository's settings page.
    Scroll down to the "Pages" section.
    Under "Source", select "gh-pages" from the dropdown menu.
    Click "Save".

This tells Github to serve your pages from the "gh-pages" branch of your repository, which is where mkdocs gh-deploy will push your documentation.

Note that it may take a few minutes for your changes to take effect on the Github Pages site.