// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Metrics Layer Documentation',
  tagline: 'The open source metrics layer',
  url: 'https://zenlytic.github.io',
  baseUrl: '/metrics_layer/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'Zenlytic', // Usually your GitHub org/user name.
  projectName: 'metrics_layer', // Usually your repo name.
  trailingSlash: false,

  presets: [
    [
      '@docusaurus/preset-classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/Zenlytic/metrics_layer/docs/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Metrics Layer',
        logo: {
          alt: 'Metrics Layer Logo',
          src: 'img/metrics-layer-logo.png',
        },
        items: [
          {
            type: 'doc',
            docId: 'getting_started',
            position: 'left',
            label: 'Tutorial',
          },
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Documentation',
          },
          {
            href: 'https://github.com/Zenlytic/metrics_layer',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Tutorial',
                to: '/docs/getting_started',
              },
              {
                label: 'Documentation',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Stack Overflow',
                href: 'https://stackoverflow.com/questions/tagged/metrics_layer',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/pablankley',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/Zenlytic/metrics_layer',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Ex Quanta, Inc. Built with Docusaurus 🦕`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
